package com.alibaba.otter.canal.deployer;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.common.utils.NamedThreadFactory;
import com.alibaba.otter.canal.instance.manager.plain.PlainCanal;
import com.alibaba.otter.canal.instance.manager.plain.PlainCanalConfigClient;

/**
 * canal独立版本启动的入口类
 * 这个类是整个canal-server的入口类。负责配置加载和启动canal-server。
 * 主要流程：
 *  - 加载 canal.properties 的配置内容
 *  - 根据 canal.admin.manager 是否为空判断是否是 admin 控制, 如果不是 admin 控制，就直接根据 canal.properties 的配置来了
 *  - 如果是 admin 控制，使用 PlainCanalConfigClient 获取远程配置 新开一个线程池每隔五秒用 http 请求去 admin 上拉配置进行 merge
 *   （这里依赖了 instance 模块的相关配置拉取的工具方法） 用 md5 进行校验，如果 canal-server 配置有更新，那么就重启 canal-server
 *  - 核心是用 canalStarter.start() 启动
 *  - 使用 CountDownLatch 保持主线程存活
 *  - 收到关闭信号，CDL-1, 然后关闭配置更新线程池，优雅退出
 *
 * @author jianghang 2012-11-6 下午05:20:49
 * @version 1.0.0
 */
public class CanalLauncher {

    private static final String             CLASSPATH_URL_PREFIX = "classpath:";
    private static final Logger             logger               = LoggerFactory.getLogger(CanalLauncher.class);
    // 运行控制latch
    public static final CountDownLatch      runningLatch         = new CountDownLatch(1);
    // 远程配置定时扫描线程池
    private static ScheduledExecutorService executor             = Executors.newScheduledThreadPool(1,
                                                                     new NamedThreadFactory("canal-server-scan"));

    /**
     * Canal Server 入口
     */
    public static void main(String[] args) {
        try {
            // region 1、设置全局异常处理器
            logger.info("## set default uncaught exception handler");
            setGlobalUncaughtExceptionHandler();
            // endregion

            // region 2、读取canal.properties文件中配置，默认读取classpath下的canal.properties
            logger.info("## load canal configurations");
            String conf = System.getProperty("canal.conf", "classpath:canal.properties");
            Properties properties = new Properties();
            if (conf.startsWith(CLASSPATH_URL_PREFIX)) {
                conf = StringUtils.substringAfter(conf, CLASSPATH_URL_PREFIX);
                properties.load(CanalLauncher.class.getClassLoader().getResourceAsStream(conf));
            } else {
                properties.load(new FileInputStream(conf));
            }
            // endregion

            // region 3、创建canalStarter，并根据是否admin控制设置属性
            // 创建启动类，根据配置设置starter属性
            final CanalStarter canalStater = new CanalStarter(properties);
            // 判断是否是admin控制，如果不是admin控制，就直接根据canal.properties的配置来
            String managerAddress = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_MANAGER);
            if (StringUtils.isNotEmpty(managerAddress)) {
                // 加载远程配置设置starter属性
                String user = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_USER);
                String passwd = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_PASSWD);
                String adminPort = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_PORT, "11110");
                boolean autoRegister = BooleanUtils.toBoolean(CanalController.getProperty(properties,
                    CanalConstants.CANAL_ADMIN_AUTO_REGISTER));
                String autoCluster = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_AUTO_CLUSTER);
                String name = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_REGISTER_NAME);
                String registerIp = CanalController.getProperty(properties, CanalConstants.CANAL_REGISTER_IP);
                if (StringUtils.isEmpty(registerIp)) {
                    registerIp = AddressUtils.getHostIp();
                }
                // 3.1 使用PlainCanalConfigClient获取远程配置
                final PlainCanalConfigClient configClient = new PlainCanalConfigClient(managerAddress,
                    user,
                    passwd,
                    registerIp,
                    Integer.parseInt(adminPort),
                    autoRegister,
                    autoCluster,
                    name);
                PlainCanal canalConfig = configClient.findServer(null);
                if (canalConfig == null) {
                    throw new IllegalArgumentException("managerAddress:" + managerAddress
                                                       + " can't not found config for [" + registerIp + ":" + adminPort
                                                       + "]");
                }
                Properties managerProperties = canalConfig.getProperties();
                // merge local
                managerProperties.putAll(properties);
                int scanIntervalInSecond = Integer.valueOf(CanalController.getProperty(managerProperties,
                    CanalConstants.CANAL_AUTO_SCAN_INTERVAL,
                    "5"));
                // 3.2 新开一个线程池每隔5秒用http请求去admin上拉配置进行merge（这里依赖了instance模块的相关配置拉取的工具方法）
                executor.scheduleWithFixedDelay(new Runnable() {

                    private PlainCanal lastCanalConfig;

                    public void run() {
                        try {
                            if (lastCanalConfig == null) {
                                lastCanalConfig = configClient.findServer(null);
                            } else {
                                // 2.3 用md5进行校验，如果canal-server配置有更新，那么就重启canal-server
                                PlainCanal newCanalConfig = configClient.findServer(lastCanalConfig.getMd5());
                                if (newCanalConfig != null) {
                                    // 远程配置canal.properties修改重新加载整个应用
                                    canalStater.stop();
                                    Properties managerProperties = newCanalConfig.getProperties();
                                    // merge local
                                    managerProperties.putAll(properties);
                                    canalStater.setProperties(managerProperties);
                                    canalStater.start();

                                    lastCanalConfig = newCanalConfig;
                                }
                            }

                        } catch (Throwable e) {
                            logger.error("scan failed", e);
                        }
                    }

                }, 0, scanIntervalInSecond, TimeUnit.SECONDS);
                canalStater.setProperties(managerProperties);
            } else {
                // 使用本地配置设置starter属性
                canalStater.setProperties(properties);
            }
            // endregion

            // region 4、启动启动类，控制程序运行和关闭
            canalStater.start();
            // CountDownLatch控制程序运行
            runningLatch.await();
            executor.shutdownNow();
            // endregion
        } catch (Throwable e) {
            logger.error("## Something goes wrong when starting up the canal Server:", e);
        }
    }

    /**
     * 设置全局未捕获异常的处理
     */
    private static void setGlobalUncaughtExceptionHandler() {
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> logger.error("UnCaughtException", e));
    }

}
