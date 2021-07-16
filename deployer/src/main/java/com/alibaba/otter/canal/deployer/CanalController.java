package com.alibaba.otter.canal.deployer;

import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import com.alibaba.otter.canal.common.zookeeper.ZookeeperPathUtils;
import com.alibaba.otter.canal.common.zookeeper.running.ServerRunningData;
import com.alibaba.otter.canal.common.zookeeper.running.ServerRunningListener;
import com.alibaba.otter.canal.common.zookeeper.running.ServerRunningMonitor;
import com.alibaba.otter.canal.common.zookeeper.running.ServerRunningMonitors;
import com.alibaba.otter.canal.deployer.InstanceConfig.InstanceMode;
import com.alibaba.otter.canal.deployer.monitor.InstanceAction;
import com.alibaba.otter.canal.deployer.monitor.InstanceConfigMonitor;
import com.alibaba.otter.canal.deployer.monitor.ManagerInstanceConfigMonitor;
import com.alibaba.otter.canal.deployer.monitor.SpringInstanceConfigMonitor;
import com.alibaba.otter.canal.instance.core.CanalInstanceGenerator;
import com.alibaba.otter.canal.instance.manager.PlainCanalInstanceGenerator;
import com.alibaba.otter.canal.instance.manager.plain.PlainCanalConfigClient;
import com.alibaba.otter.canal.instance.spring.SpringCanalInstanceGenerator;
import com.alibaba.otter.canal.server.CanalMQStarter;
import com.alibaba.otter.canal.server.embedded.CanalServerWithEmbedded;
import com.alibaba.otter.canal.server.exception.CanalServerException;
import com.alibaba.otter.canal.server.netty.CanalServerWithNetty;
import com.google.common.base.Function;
import com.google.common.collect.MapMaker;
import com.google.common.collect.MigrateMap;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.Map;
import java.util.Properties;

/**
 * canal调度控制器
 *
 * @author jianghang 2012-11-8 下午12:03:11
 * @version 1.0.0
 */
public class CanalController {

    private static final Logger                      logger   = LoggerFactory.getLogger(CanalController.class);
    private String                                   ip;
    private String                                   registerIp;
    private int                                      port;
    private int                                      adminPort;
    // 默认使用spring的方式载入
    private Map<String, InstanceConfig>              instanceConfigs;           // 每个instance配置
    private InstanceConfig                           globalInstanceConfig;      // instance的全局配置
    private Map<String, PlainCanalConfigClient>      managerClients;
    // 监听instance config的变化
    private boolean                                  autoScan = true;
    private InstanceAction                           defaultAction;             // 定义destination配置变化时的默认行为
    private Map<InstanceMode, InstanceConfigMonitor> instanceConfigMonitors;    // 用于监听destination配置是否发生了变化
    private CanalServerWithEmbedded                  embededCanalServer;        // 生成instance，真正处理客户端请求
    private CanalServerWithNetty                     canalServer;               // 用于与客户端通信，将请求交给embededCanalServer处理

    private CanalInstanceGenerator                   instanceGenerator;         // 用于创建CanalInstance实例
    private ZkClientx                                zkclientx;

    private CanalMQStarter                           canalMQStarter;
    private String                                   adminUser;
    private String                                   adminPasswd;

    public CanalController(){
        this(System.getProperties());
    }

    /**
     * 构造，在 CanalStarter 中调用，主要初始化配置和准备 CanalServer，以及设置 instance 的监控等
     * @param properties canal文件配置
     */
    public CanalController(final Properties properties) {
        // region 1、构建 PlainCanalConfigClient，用于用户远程配置的获取
        managerClients = MigrateMap.makeComputingMap(this::getManagerClient);
        // endregion

        // region 2、初始化全局参数设置
        globalInstanceConfig = initGlobalConfig(properties);
        instanceConfigs = new MapMaker().makeMap();
        // 初始化单个instance的config
        initInstanceConfig(properties);

        // init socketChannel
        String socketChannel = getProperty(properties, CanalConstants.CANAL_SOCKETCHANNEL);
        if (StringUtils.isNotEmpty(socketChannel)) {
            System.setProperty(CanalConstants.CANAL_SOCKETCHANNEL, socketChannel);
        }

        // 兼容1.1.0版本的ak/sk参数名
        String accesskey = getProperty(properties, "canal.instance.rds.accesskey");
        String secretkey = getProperty(properties, "canal.instance.rds.secretkey");
        if (StringUtils.isNotEmpty(accesskey)) {
            System.setProperty(CanalConstants.CANAL_ALIYUN_ACCESSKEY, accesskey);
        }
        if (StringUtils.isNotEmpty(secretkey)) {
            System.setProperty(CanalConstants.CANAL_ALIYUN_SECRETKEY, secretkey);
        }
        // endregion

        // region 3、准备canal server，核心在于 embededCanalServer
        // 核心在于embededCanalServer，如果有需要canalServerWithNetty，那就多包装一个（serverMode=mq的情况不需要netty）
        ip = getProperty(properties, CanalConstants.CANAL_IP);
        registerIp = getProperty(properties, CanalConstants.CANAL_REGISTER_IP);
        port = Integer.valueOf(getProperty(properties, CanalConstants.CANAL_PORT, "11111"));
        adminPort = Integer.valueOf(getProperty(properties, CanalConstants.CANAL_ADMIN_PORT, "11110"));
        /*
         之后分别为以下两个字段赋值：
         embededCanalServer：类型为CanalServerWithEmbedded
         canalServer：类型为CanalServerWithNetty
         上述两个类型都实现自CanalServer接口，之间关关系见官方文档 https://github.com/alibaba/canal/wiki/%E7%AE%80%E4%BB%8B#server%E8%AE%BE%E8%AE%A1
          
         说白了，就是我们可以不必独立部署canal server。在应用直接使用CanalServerWithEmbedded直连mysql数据库。
         如果觉得自己的技术hold不住相关代码，就独立部署一个canal server，使用canal提供的客户端，连接canal server获取binlog解析后数据。
         而CanalServerWithNetty是在CanalServerWithEmbedded的基础上做的一层封装，用于与客户端通信。
          
         在独立部署canal server时，Canal客户端发送的所有请求都交给CanalServerWithNetty处理解析，
         解析完成之后委派给了交给CanalServerWithEmbedded进行处理。
         因此CanalServerWithNetty就是一个马甲而已。CanalServerWithEmbedded才是核心。
         
         因此，在上述代码中，我们看到，用于生成CanalInstance实例的instanceGenerator被设置到了CanalServerWithEmbedded中，
         而ip和port被设置到CanalServerWithNetty中。
         
         关于CanalServerWithNetty如何将客户端的请求委派给CanalServerWithEmbedded进行处理，我们将在server模块源码分析中进行讲解。
         */
        embededCanalServer = CanalServerWithEmbedded.instance();
        embededCanalServer.setCanalInstanceGenerator(instanceGenerator);// 设置自定义的instanceGenerator
        int metricsPort = Integer.valueOf(getProperty(properties, CanalConstants.CANAL_METRICS_PULL_PORT, "11112"));
        embededCanalServer.setMetricsPort(metricsPort);

        this.adminUser = getProperty(properties, CanalConstants.CANAL_ADMIN_USER);
        this.adminPasswd = getProperty(properties, CanalConstants.CANAL_ADMIN_PASSWD);
        embededCanalServer.setUser(getProperty(properties, CanalConstants.CANAL_USER));
        embededCanalServer.setPasswd(getProperty(properties, CanalConstants.CANAL_PASSWD));

        // 如果有需要 canalServerWithNetty，那就多包装一个（我们 serverMode=mq 是不需要这个 netty 的）
        // 这里如果没有在CanalStarter中将canalWithoutNetty设置为true，即没有使用mq，则初始化CanalServerWithNetty对象，接收客户端请求
        String canalWithoutNetty = getProperty(properties, CanalConstants.CANAL_WITHOUT_NETTY);
        if (canalWithoutNetty == null || "false".equals(canalWithoutNetty)) {
            canalServer = CanalServerWithNetty.instance();
            canalServer.setIp(ip);
            canalServer.setPort(port);
        }
        // endregion

        // region 4、初始化 zkClient
        // 处理下ip为空，默认使用hostIp暴露到zk中
        if (StringUtils.isEmpty(ip) && StringUtils.isEmpty(registerIp)) {
            ip = registerIp = AddressUtils.getHostIp();
        }
        if (StringUtils.isEmpty(ip)) {
            ip = AddressUtils.getHostIp();
        }
        if (StringUtils.isEmpty(registerIp)) {
            registerIp = ip; // 兼容以前配置
        }
        
        // 读取canal.properties中的配置项canal.zkServers，如果没有这个配置，则表示项目不使用zk
        // canal支持利用了zk来完成HA机制、以及将当前消费到到的mysql的binlog位置记录到zk中
        final String zkServers = getProperty(properties, CanalConstants.CANAL_ZKSERVERS);
        if (StringUtils.isNotEmpty(zkServers)) {
            // 创建zk实例
            zkclientx = ZkClientx.getZkClient(zkServers);
            // 初始化系统目录
            zkclientx.createPersistent(ZookeeperPathUtils.DESTINATION_ROOT_NODE, true);   // destination列表
            zkclientx.createPersistent(ZookeeperPathUtils.CANAL_CLUSTER_ROOT_NODE, true); // canal server的集群列表
        }
        // endregion

        // region 5、初始化 ServerRunningMonitors，作为 instance 运行节点控制，定义了instance的启动停止方法
        // CanalInstance运行状态监控
        final ServerRunningData serverData = new ServerRunningData(registerIp + ":" + port);
        // 设置CanalServer信息
        ServerRunningMonitors.setServerData(serverData);
        // 设置用于监控CanalInstance的ServerRunningMonitor映射，使用MigrateMap.makeComputingMap，获取时创建
        ServerRunningMonitors.setRunningMonitors(MigrateMap.makeComputingMap((Function<String, ServerRunningMonitor>) destination -> {
            // 根据 ServerRunningData 创建 ServerRunningMonitor 实例
            ServerRunningMonitor runningMonitor = new ServerRunningMonitor(serverData);
            // 设置 destination 
            runningMonitor.setDestination(destination);
            /**
             * 设置 ServerRunningListener 对象
             * ServerRunningListener 是个接口，这里采用了匿名内部类的形式构建，实现了各个接口的方法。
             * 
             * 主要为 instance 在当前 server 上的状态发生变化时调用。比如要在当前 server 上启动这个 instance 了，
             * 就调用相关启动方法，如果在这个 server 上关闭 instance，就调用相关关闭方法。
             */
            runningMonitor.setListener(new ServerRunningListener() {
                
                /**
                 * 内部调用了 embededCanalServer 的 start(destination) 方法。
                 * 
                 * 此处重点，说明每个 destination 对应的 CanalInstance 是通过 embededCanalServer 的 start 方法启动的，
                 * 这与我们之前分析将 instanceGenerator 设置到 embededCanalServer 中可以对应上。
                 * embededCanalServer 负责调用 instanceGenerator 生成 CanalInstance 实例，并负责其启动。
                 * 
                 * 如果投递mq，还会直接调用 canalMQStarter 来启动一个 destination 的工作线程。
                 * 
                 * 调用处 {@link com.alibaba.otter.canal.common.zookeeper.running.ServerRunningMonitor#start}
                 */
                public void processActiveEnter() {
                    try {
                        MDC.put(CanalConstants.MDC_DESTINATION, String.valueOf(destination));
                        // 启动 embededCanalServer 中的 destination
                        embededCanalServer.start(destination);
                        if (canalMQStarter != null) {
                            canalMQStarter.startDestination(destination);
                        }
                    } finally {
                        MDC.remove(CanalConstants.MDC_DESTINATION);
                    }
                }
                
                /**
                 * 内部调用 embededCanalServer 的 stop(destination) 方法。与上 start 方法类似，只不过是停止 CanalInstance。
                 * 与开始顺序相反，如果有 mqStarter，先停止 mqStarter 的 destination 工作线程。
                 * 停止 embedeCanalServer 的 destination。
                 *
                 * 调用处 {@link com.alibaba.otter.canal.common.zookeeper.running.ServerRunningMonitor#stop}
                 */
                public void processActiveExit() {
                    try {
                        MDC.put(CanalConstants.MDC_DESTINATION, String.valueOf(destination));
                        if (canalMQStarter != null) {
                            canalMQStarter.stopDestination(destination);
                        }
                        embededCanalServer.stop(destination);
                    } finally {
                        MDC.remove(CanalConstants.MDC_DESTINATION);
                    }
                }

                /**
                 * 处理存在zk的情况下，在CanalInstance启动之前，在zk中创建节点。
                 * 路径为：/otter/canal/destinations/{destination}/cluster/{port}。
                 * 此方法会在processActiveEnter()之前被调用。
                 */
                public void processStart() {
                    try {
                        if (zkclientx != null) {
                            final String path = ZookeeperPathUtils.getDestinationClusterNode(destination,
                                registerIp + ":" + port);
                            initCid(path);
                            zkclientx.subscribeStateChanges(new IZkStateListener() {

                                public void handleStateChanged(KeeperState state) throws Exception {

                                }

                                public void handleNewSession() throws Exception {
                                    initCid(path);
                                }

                                @Override
                                public void handleSessionEstablishmentError(Throwable error) throws Exception {
                                    logger.error("failed to connect to zookeeper", error);
                                }
                            });
                        }
                    } finally {
                        MDC.remove(CanalConstants.MDC_DESTINATION);
                    }
                }

                /**
                 * 处理存在zk的情况下，在CanalInstance停止前，释放zk节点。
                 * 路径为：/otter/canal/destinations/{destination}/cluster/{port}。
                 * 此方法会在processActiveExit()之后被调用。
                 */
                public void processStop() {
                    try {
                        MDC.put(CanalConstants.MDC_DESTINATION, String.valueOf(destination));
                        if (zkclientx != null) {
                            final String path = ZookeeperPathUtils.getDestinationClusterNode(destination,
                                registerIp + ":" + port);
                            releaseCid(path);
                        }
                    } finally {
                        MDC.remove(CanalConstants.MDC_DESTINATION);
                    }
                }

            });
            // 如果 zkclientx 不为空，也设置到 ServerRunningMonitor 中
            if (zkclientx != null) {
                runningMonitor.setZkClient(zkclientx);
            }
            // 触发创建一下cid节点，调用的就是 ServerRunningListener 的 processStart 方法
            runningMonitor.init();
            return runningMonitor;
        }));
        // endregion

        // region 6、初始化 InstanceAction，完成 monitor 机制。（监控 instance 配置变化然后调用 ServerRunningMonitor 进行处理）
        autoScan = BooleanUtils.toBoolean(getProperty(properties, CanalConstants.CANAL_AUTO_SCAN));
        if (autoScan) {
            // 设置defaultAction，定义destination配置变化时的默认行为
            defaultAction = new InstanceAction() {

                // 当新增一个 destination 配置时，需要调用 start 方法来启动
                @Override
                public void start(String destination) {
                    InstanceConfig config = instanceConfigs.get(destination);
                    if (config == null) {
                        // 重新读取一下instance config
                        config = parseInstanceConfig(properties, destination);
                        instanceConfigs.put(destination, config);
                    }

                    if (!embededCanalServer.isStart(destination)) {
                        // HA机制启动
                        ServerRunningMonitor runningMonitor = ServerRunningMonitors.getRunningMonitor(destination);
                        if (!config.getLazy() && !runningMonitor.isStart()) {
                            runningMonitor.start();
                        }
                    }

                    logger.info("auto notify start {} successful.", destination);
                }

                // 当移除一个 destination 配置时，需要调用 stop 方法来停止
                @Override
                public void stop(String destination) {
                    // 此处的stop，代表强制退出，非HA机制，所以需要退出HA的monitor和配置信息
                    InstanceConfig config = instanceConfigs.remove(destination);
                    if (config != null) {
                        embededCanalServer.stop(destination);
                        ServerRunningMonitor runningMonitor = ServerRunningMonitors.getRunningMonitor(destination);
                        if (runningMonitor.isStart()) {
                            runningMonitor.stop();
                        }
                    }

                    logger.info("auto notify stop {} successful.", destination);
                }

                // 当某个 destination 配置发生变更时，需要调用 reload 方法来进行重启
                @Override
                public void reload(String destination) {
                    // 目前任何配置变化，直接重启，简单处理
                    stop(destination);
                    start(destination);

                    logger.info("auto notify reload {} successful.", destination);
                }

                // 主动释放destination运行
                @Override
                public void release(String destination) {
                    // 此处的release，代表强制释放，主要针对HA机制释放运行，让给其他机器抢占
                    InstanceConfig config = instanceConfigs.get(destination);
                    if (config != null) {
                        ServerRunningMonitor runningMonitor = ServerRunningMonitors.getRunningMonitor(destination);
                        if (runningMonitor.isStart()) {
                            boolean release = runningMonitor.release();
                            if (!release) {
                                // 如果是单机模式,则直接清除配置
                                instanceConfigs.remove(destination);
                                // 停掉服务
                                runningMonitor.stop();
                                if (instanceConfigMonitors.containsKey(InstanceConfig.InstanceMode.MANAGER)) {
                                    ManagerInstanceConfigMonitor monitor = (ManagerInstanceConfigMonitor) instanceConfigMonitors.get(InstanceConfig.InstanceMode.MANAGER);
                                    Map<String, InstanceAction> instanceActions = monitor.getActions();
                                    if (instanceActions.containsKey(destination)) {
                                        // 清除内存中的autoScan cache
                                        monitor.release(destination);
                                    }
                                }
                            }
                        }
                    }

                    logger.info("auto notify release {} successful.", destination);
                }
            };

            // 设置instanceConfigMonitors，监听配置变化，针对两种不同的配置加载方式
            instanceConfigMonitors = MigrateMap.makeComputingMap(mode -> {
                int scanInterval = Integer.valueOf(getProperty(properties,
                    CanalConstants.CANAL_AUTO_SCAN_INTERVAL,
                    "5"));

                if (mode.isSpring()) {
                    // 如果加载方式是spring，返回SpringInstanceConfigMonitor
                    SpringInstanceConfigMonitor monitor = new SpringInstanceConfigMonitor();
                    monitor.setScanIntervalInSecond(scanInterval);
                    monitor.setDefaultAction(defaultAction);
                    // 设置conf目录，默认是user.dir + conf目录组成
                    String rootDir = getProperty(properties, CanalConstants.CANAL_CONF_DIR);
                    if (StringUtils.isEmpty(rootDir)) {
                        rootDir = "../conf";
                    }

                    if (StringUtils.equals("otter-canal", System.getProperty("appName"))) {
                        monitor.setRootConf(rootDir);
                    } else {
                        // eclipse debug模式
                        monitor.setRootConf("src/main/resources/");
                    }
                    return monitor;
                } else if (mode.isManager()) {
                    // 如果加载方式是manager，返回ManagerInstanceConfigMonitor
                    ManagerInstanceConfigMonitor monitor = new ManagerInstanceConfigMonitor();
                    monitor.setScanIntervalInSecond(scanInterval);
                    monitor.setDefaultAction(defaultAction);
                    String managerAddress = getProperty(properties, CanalConstants.CANAL_ADMIN_MANAGER);
                    monitor.setConfigClient(getManagerClient(managerAddress));
                    return monitor;
                } else {
                    throw new UnsupportedOperationException("unknow mode :" + mode + " for monitor");
                }
            });
        }
        // endregion
    }

    /**
     * 读取相关全局配置，设置到全局 InstanceConfig 中，并初始化 instanceGenerator
     * @param properties canal文件配置
     * @return 返回加载到的全局配置
     */
    private InstanceConfig initGlobalConfig(Properties properties) {
        // region 1、读取各项全局配置，设置到 globalConfig 中
        String adminManagerAddress = getProperty(properties, CanalConstants.CANAL_ADMIN_MANAGER);
        InstanceConfig globalConfig = new InstanceConfig();
        // 1.1 读取canal.instance.global.mode
        String modeStr = getProperty(properties, CanalConstants.getInstanceModeKey(CanalConstants.GLOBAL_NAME));
        if (StringUtils.isNotEmpty(adminManagerAddress)) {
            // 如果指定了manager地址,则强制适用manager
            globalConfig.setMode(InstanceMode.MANAGER);
        } else if (StringUtils.isNotEmpty(modeStr)) {
            globalConfig.setMode(InstanceMode.valueOf(StringUtils.upperCase(modeStr)));
        }

        // 1.2 读取canal.instance.global.lazy
        String lazyStr = getProperty(properties, CanalConstants.getInstancLazyKey(CanalConstants.GLOBAL_NAME));
        if (StringUtils.isNotEmpty(lazyStr)) {
            globalConfig.setLazy(Boolean.valueOf(lazyStr));
        }

        // 1.3 读取canal.instance.global.manager.address
        String managerAddress = getProperty(properties,
            CanalConstants.getInstanceManagerAddressKey(CanalConstants.GLOBAL_NAME));
        if (StringUtils.isNotEmpty(managerAddress)) {
            if (StringUtils.equals(managerAddress, "${canal.admin.manager}")) {
                managerAddress = adminManagerAddress;
            }

            globalConfig.setManagerAddress(managerAddress);
        }

        // 1.4 读取canal.instance.global.spring.xml
        String springXml = getProperty(properties, CanalConstants.getInstancSpringXmlKey(CanalConstants.GLOBAL_NAME));
        if (StringUtils.isNotEmpty(springXml)) {
            globalConfig.setSpringXml(springXml);
        }
        // endregion

        // region 2、初始化instanceGenerator
        instanceGenerator = destination -> {
            // 2.1 获取指定 destination 的 instanceConfig
            InstanceConfig config = instanceConfigs.get(destination);
            if (config == null) {
                throw new CanalServerException("can't find destination:" + destination);
            }

            // 2.2 如果 canal.instance.global.mode = manager，使用 PlainCanalInstanceGenerator
            if (config.getMode().isManager()) {
                PlainCanalInstanceGenerator instanceGenerator = new PlainCanalInstanceGenerator(properties);
                instanceGenerator.setCanalConfigClient(managerClients.get(config.getManagerAddress()));
                instanceGenerator.setSpringXml(config.getSpringXml());  // 还是设置了springXml,内部获取instance还是走了beanFactory的方式
                return instanceGenerator.generate(destination);
            } else if (config.getMode().isSpring()) {
                // 2.3 如果 canal.instance.global.mode = spring，使用 SpringCanalInstanceGenerator
                SpringCanalInstanceGenerator instanceGenerator = new SpringCanalInstanceGenerator();
                instanceGenerator.setSpringXml(config.getSpringXml());
                return instanceGenerator.generate(destination);
            } else {
                throw new UnsupportedOperationException("unknow mode :" + config.getMode());
            }

        };
        // endregion

        return globalConfig;
    }

    /**
     * 创建获取远程配置的客户端
     * @param managerAddress 远程地址
     * @return 返回获取配置客户端
     */
    private PlainCanalConfigClient getManagerClient(String managerAddress) {
        return new PlainCanalConfigClient(managerAddress, this.adminUser, this.adminPasswd, this.registerIp, adminPort);
    }

    /**
     * 加载每个 destination 的配置，添加到 instanceConfigs 中
     * @param properties canal文件配置
     */
    private void initInstanceConfig(Properties properties) {
        // 以","分割canal.destinations，得到一个数组形式的destination
        String destinationStr = getProperty(properties, CanalConstants.CANAL_DESTINATIONS);
        String[] destinations = StringUtils.split(destinationStr, CanalConstants.CANAL_DESTINATION_SPLIT);
        // 逐个解析instance配置，放到配置map中
        for (String destination : destinations) {
            // 为每一个destination生成一个InstanceConfig实例
            InstanceConfig config = parseInstanceConfig(properties, destination);
            // 将destination对应的InstanceConfig放入instanceConfigs中
            InstanceConfig oldConfig = instanceConfigs.put(destination, config);

            if (oldConfig != null) {
                logger.warn("destination:{} old config:{} has replace by new config:{}", destination, oldConfig, config);
            }
        }
    }

    /**
     * 根据instance名称解析指定的instance配置
     * @param properties  总体配置
     * @param destination instance名称
     * @return 返回封装了配置的InstanceConfig对象
     */
    private InstanceConfig parseInstanceConfig(Properties properties, String destination) {
        String adminManagerAddress = getProperty(properties, CanalConstants.CANAL_ADMIN_MANAGER);
        // 每个destination对应的InstanceConfig都引用了全局的globalInstanceConfig
        InstanceConfig config = new InstanceConfig(globalInstanceConfig);
        
        // 以下用来设置config的方式和设置全局配置类似，对应配置的key不同而已
        String modeStr = getProperty(properties, CanalConstants.getInstanceModeKey(destination));
        if (StringUtils.isNotEmpty(adminManagerAddress)) {
            // 如果指定了manager地址,则强制适用manager
            config.setMode(InstanceMode.MANAGER);
        } else if (StringUtils.isNotEmpty(modeStr)) {
            config.setMode(InstanceMode.valueOf(StringUtils.upperCase(modeStr)));
        }

        String lazyStr = getProperty(properties, CanalConstants.getInstancLazyKey(destination));
        if (!StringUtils.isEmpty(lazyStr)) {
            config.setLazy(Boolean.valueOf(lazyStr));
        }

        if (config.getMode().isManager()) {
            String managerAddress = getProperty(properties, CanalConstants.getInstanceManagerAddressKey(destination));
            if (StringUtils.isNotEmpty(managerAddress)) {
                if (StringUtils.equals(managerAddress, "${canal.admin.manager}")) {
                    managerAddress = adminManagerAddress;
                }
                config.setManagerAddress(managerAddress);
            }
        }

        String springXml = getProperty(properties, CanalConstants.getInstancSpringXmlKey(destination));
        if (StringUtils.isNotEmpty(springXml)) {
            config.setSpringXml(springXml);
        }

        return config;
    }

    public static String getProperty(Properties properties, String key, String defaultValue) {
        String value = getProperty(properties, key);
        if (StringUtils.isEmpty(value)) {
            return defaultValue;
        } else {
            return value;
        }
    }

    public static String getProperty(Properties properties, String key) {
        key = StringUtils.trim(key);
        String value = System.getProperty(key);

        if (value == null) {
            value = System.getenv(key);
        }

        if (value == null) {
            value = properties.getProperty(key);
        }

        return StringUtils.trim(value);
    }

    /**
     * 启动方法，负责创建zk工作节点，启动embededCanalServer，逐个启动instance并注册配置监听
     * @throws Throwable
     */
    public void start() throws Throwable {
        logger.info("## start the canal server[{}({}):{}]", ip, registerIp, port);

        // region 1、在 zk 的 /otter/canal/cluster 目录下根据 ip:port 创建 server 的临时节点，注册zk监听器
        // 创建整个canal的工作节点
        final String path = ZookeeperPathUtils.getCanalClusterNode(registerIp + ":" + port);
        initCid(path);
        if (zkclientx != null) {  // 注册zk监听
            this.zkclientx.subscribeStateChanges(new IZkStateListener() {

                public void handleStateChanged(KeeperState state) throws Exception {

                }

                public void handleNewSession() throws Exception {
                    initCid(path);
                }

                @Override
                public void handleSessionEstablishmentError(Throwable error) throws Exception {
                    logger.error("failed to connect to zookeeper", error);
                }
            });
        }
        // endregion

        // region 2、优先启动embeded服务
        embededCanalServer.start();
        // endregion

        // region 3、根据配置的 instance 的 destination，调用 runningMonitor.start() 逐个启动 instance
        // 尝试启动一下非lazy状态的通道
        // 启动不是lazy模式的CanalInstance，通过遍历instanceConfigs，根据destination获取对应的ServerRunningMonitor，然后逐一启动
        for (Map.Entry<String, InstanceConfig> entry : instanceConfigs.entrySet()) {
            final String destination = entry.getKey();
            InstanceConfig config = entry.getValue();
            // 创建destination的工作节点，如果destination对应的CanalInstance没有启动，则进行启动
            if (!embededCanalServer.isStart(destination)) {
                // HA机制启动
                ServerRunningMonitor runningMonitor = ServerRunningMonitors.getRunningMonitor(destination);
                // 如果不是lazy，lazy模式需要等到第一次有客户端请求才会启动
                if (!config.getLazy() && !runningMonitor.isStart()) {
                    runningMonitor.start();  // ** 真正的启动 canal instance
                }
            }

            // 为每个instance注册一个配置监视器
            if (autoScan) {
                instanceConfigMonitors.get(config.getMode()).register(destination, defaultAction);
            }
        }

        // 启动配置文件自动检测机制
        if (autoScan) {
            // 启动线程定时去扫描配置
            instanceConfigMonitors.get(globalInstanceConfig.getMode()).start();
            for (InstanceConfigMonitor monitor : instanceConfigMonitors.values()) {
                if (!monitor.isStart()) {
                    monitor.start(); // 启动monitor
                }
            }
        }
        // endregion

        // region 4、如果 cannalServer 不为空，启动 canServer (canalServerWithNetty)
        if (canalServer != null) {
            canalServer.start();   // 启动网络接口，监听客户端请求，mq模式下为空
        }
        // endregion
    }

    public void stop() throws Throwable {

        if (canalServer != null) {
            canalServer.stop();
        }

        if (autoScan) {
            for (InstanceConfigMonitor monitor : instanceConfigMonitors.values()) {
                if (monitor.isStart()) {
                    monitor.stop();
                }
            }
        }

        for (ServerRunningMonitor runningMonitor : ServerRunningMonitors.getRunningMonitors().values()) {
            if (runningMonitor.isStart()) {
                runningMonitor.stop();
            }
        }

        // 释放canal的工作节点
        releaseCid(ZookeeperPathUtils.getCanalClusterNode(registerIp + ":" + port));
        logger.info("## stop the canal server[{}({}):{}]", ip, registerIp, port);

        if (zkclientx != null) {
            zkclientx.close();
        }

        // 关闭时清理缓存
        if (instanceConfigs != null) {
            instanceConfigs.clear();
        }
        if (managerClients != null) {
            managerClients.clear();
        }
        if (instanceConfigMonitors != null) {
            instanceConfigMonitors.clear();
        }

        ZkClientx.clearClients();
    }

    private void initCid(String path) {
        // logger.info("## init the canalId = {}", cid);
        // 初始化系统目录
        if (zkclientx != null) {
            try {
                zkclientx.createEphemeral(path);
            } catch (ZkNoNodeException e) {
                // 如果父目录不存在，则创建
                String parentDir = path.substring(0, path.lastIndexOf('/'));
                zkclientx.createPersistent(parentDir, true);
                zkclientx.createEphemeral(path);
            } catch (ZkNodeExistsException e) {
                // ignore
                // 因为第一次启动时创建了cid,但在stop/start的时可能会关闭和新建,允许出现NodeExists问题s
            }

        }
    }

    private void releaseCid(String path) {
        // logger.info("## release the canalId = {}", cid);
        // 初始化系统目录
        if (zkclientx != null) {
            zkclientx.delete(path);
        }
    }

    public CanalMQStarter getCanalMQStarter() {
        return canalMQStarter;
    }

    public void setCanalMQStarter(CanalMQStarter canalMQStarter) {
        this.canalMQStarter = canalMQStarter;
    }

    public Map<InstanceMode, InstanceConfigMonitor> getInstanceConfigMonitors() {
        return instanceConfigMonitors;
    }

    public Map<String, InstanceConfig> getInstanceConfigs() {
        return instanceConfigs;
    }

}
