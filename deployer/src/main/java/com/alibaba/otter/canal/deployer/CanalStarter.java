package com.alibaba.otter.canal.deployer;

import java.util.Properties;

import com.alibaba.otter.canal.connector.core.config.MQProperties;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.admin.netty.CanalAdminWithNetty;
import com.alibaba.otter.canal.connector.core.spi.CanalMQProducer;
import com.alibaba.otter.canal.connector.core.spi.ExtensionLoader;
import com.alibaba.otter.canal.deployer.admin.CanalAdminController;
import com.alibaba.otter.canal.server.CanalMQStarter;

/**
 * Canal server 启动类
 * 这个类是整个canal-server的启动类，负责创建核心的CanalController，主要流程如下：
 * - 根据配置的 serverMode，决定使用 CanalMQProducer 或者 canalServerWithNetty
 * - 启动 CanalController
 * - 注册 shutdownHook
 * - 如果 CanalMQProducer 不为空，启动 canalMQStarter（内部使用 CanalMQProducer 将消息投递给 mq）
 * - 启动 CanalAdminWithNetty 做服务器
 *
 * @author rewerma 2020-01-27
 * @version 1.0.2
 */
public class CanalStarter {

    private static final Logger logger                    = LoggerFactory.getLogger(CanalStarter.class);

    private static final String CONNECTOR_SPI_DIR         = "/plugin";
    private static final String CONNECTOR_STANDBY_SPI_DIR = "/canal/plugin";

    private CanalController     controller                = null;
    private CanalMQProducer     canalMQProducer           = null;
    private Thread              shutdownThread            = null;
    private CanalMQStarter      canalMQStarter            = null;
    private volatile Properties properties;
    private volatile boolean    running                   = false;

    private CanalAdminWithNetty canalAdmin;

    public CanalStarter(Properties properties){
        this.properties = properties;
    }

    public boolean isRunning() {
        return running;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public CanalController getController() {
        return controller;
    }

    /**
     * 启动方法
     *
     * @throws Throwable
     */
    public synchronized void start() throws Throwable {
        // region 1、如果canal.serverMode不是tcp，加载CanalMQProducer，并且初始化CanalMQProducer
        String serverMode = CanalController.getProperty(properties, CanalConstants.CANAL_SERVER_MODE);
        if (!"tcp".equalsIgnoreCase(serverMode)) {  // 不是tcp模式，加载外部拓展
            // 得到扩展加载器
            ExtensionLoader<CanalMQProducer> loader = ExtensionLoader.getExtensionLoader(CanalMQProducer.class);
            // 根据 serverMode（kafka,rocketmq,rabbitmq），用加载器获取 MQProducer
            canalMQProducer = loader
                .getExtension(serverMode.toLowerCase(), CONNECTOR_SPI_DIR, CONNECTOR_STANDBY_SPI_DIR);
            if (canalMQProducer != null) {
                ClassLoader cl = Thread.currentThread().getContextClassLoader();
                Thread.currentThread().setContextClassLoader(canalMQProducer.getClass().getClassLoader());
                // 初始化 MQProducer 成员，得到连接好的消息生产者
                canalMQProducer.init(properties);
                Thread.currentThread().setContextClassLoader(cl);
            }
        }

        // 如果启动了canalMQProducer，就不使用canalWithNetty
        // canalWithNetty负责和和canalClient通信，接收请求
        if (canalMQProducer != null) {
            MQProperties mqProperties = canalMQProducer.getMqProperties();
            // disable netty
            System.setProperty(CanalConstants.CANAL_WITHOUT_NETTY, "true");
            if (mqProperties.isFlatMessage()) {
                // 设置为raw避免ByteString->Entry的二次解析
                System.setProperty("canal.instance.memory.rawEntry", "false");
            }
        }

        logger.info("## start the canal server.");
        // endregion

        // region 2、启动CanalController
        controller = new CanalController(properties);
        controller.start();
        logger.info("## the canal server is running now ......");
        // endregion

        // region 3、注册了一个shutdownHook,系统退出时执行相关逻辑
        shutdownThread = new Thread(() -> {
            try {
                logger.info("## stop the canal server");
                controller.stop();
                CanalLauncher.runningLatch.countDown();
            } catch (Throwable e) {
                logger.warn("##something goes wrong when stopping canal Server:", e);
            } finally {
                logger.info("## canal server is down.");
            }
        });
        Runtime.getRuntime().addShutdownHook(shutdownThread);
        // endregion

        // region 4、初始化并启动canalMQStarter，集群版的话，没有预先配置destinations
        if (canalMQProducer != null) {
            canalMQStarter = new CanalMQStarter(canalMQProducer);
            String destinations = CanalController.getProperty(properties, CanalConstants.CANAL_DESTINATIONS);
            // 启动 MQ 生产者，根据 instance
            canalMQStarter.start(destinations);
            controller.setCanalMQStarter(canalMQStarter);
        }
        // endregion

        // region 5、根据填写的canalAdmin的ip和port，启动canalAdmin
        // start canalAdmin
        String port = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_PORT);
        if (canalAdmin == null && StringUtils.isNotEmpty(port)) {
            String user = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_USER);
            String passwd = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_PASSWD);
            // 创建CanalAdminController，接收控制台请求，返回运行状态
            CanalAdminController canalAdmin = new CanalAdminController(this);
            canalAdmin.setUser(user);
            canalAdmin.setPasswd(passwd);

            String ip = CanalController.getProperty(properties, CanalConstants.CANAL_IP);

            logger.debug("canal admin port:{}, canal admin user:{}, canal admin password: {}, canal ip:{}",
                port,
                user,
                passwd,
                ip);

            CanalAdminWithNetty canalAdminWithNetty = CanalAdminWithNetty.instance();
            canalAdminWithNetty.setCanalAdmin(canalAdmin);
            canalAdminWithNetty.setPort(Integer.parseInt(port));
            canalAdminWithNetty.setIp(ip);
            canalAdminWithNetty.start();
            this.canalAdmin = canalAdminWithNetty;
        }
        // endregion

        // 设置启动状态
        running = true;
    }

    public synchronized void stop() throws Throwable {
        stop(false);
    }

    /**
     * 销毁方法，远程配置变更时调用
     *
     * @throws Throwable
     */
    public synchronized void stop(boolean stopByAdmin) throws Throwable {
        if (!stopByAdmin && canalAdmin != null) {
            canalAdmin.stop();
            canalAdmin = null;
        }

        if (controller != null) {
            controller.stop();
            controller = null;
        }
        if (shutdownThread != null) {
            Runtime.getRuntime().removeShutdownHook(shutdownThread);
            shutdownThread = null;
        }
        if (canalMQProducer != null && canalMQStarter != null) {
            canalMQStarter.destroy();
            canalMQStarter = null;
            canalMQProducer = null;
        }
        running = false;
    }
}
