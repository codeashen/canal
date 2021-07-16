package com.alibaba.otter.canal.common.zookeeper.running;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.exception.ZkException;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.common.utils.BooleanMutex;
import com.alibaba.otter.canal.common.utils.JsonUtils;
import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import com.alibaba.otter.canal.common.zookeeper.ZookeeperPathUtils;

/**
 * 针对server的running节点控制
 * 主要用来记录每个 instance 的运行状态数据
 * 
 * @author jianghang 2012-11-22 下午02:59:42
 * @version 1.0.0
 */
public class ServerRunningMonitor extends AbstractCanalLifeCycle {

    private static final Logger        logger       = LoggerFactory.getLogger(ServerRunningMonitor.class);
    private ZkClientx                  zkClient;
    private String                     destination;
    private IZkDataListener            dataListener;        // destination在zk上的running节点监听
    private BooleanMutex               mutex        = new BooleanMutex(false);
    private volatile boolean           release      = false;
    // 当前服务节点状态信息
    private ServerRunningData          serverData;
    // 当前实际运行的节点状态信息
    private volatile ServerRunningData activeData;
    private ScheduledExecutorService   delayExector = Executors.newScheduledThreadPool(1);
    private int                        delayTime    = 5;
    private ServerRunningListener      listener;

    public ServerRunningMonitor(ServerRunningData serverData){
        this();
        this.serverData = serverData;
    }

    /**
     * 构造方法就干一件事：初始化 destination 对应的 running 节点监听器。
     * 监听对应的 canal server 启动状态。
     * 一旦发生了变化，则说明可能其他处理相同 destination 的 canal server 可能出现了异常，此时需要尝试自己进入running状态。
     */
    public ServerRunningMonitor(){
        // 封装zk节点数据发生变化的回调
        // 创建父节点
        dataListener = new IZkDataListener() {

            /**
             * 当注册节点发生变化时，会自动回调此方法
             */
            public void handleDataChange(String dataPath, Object data) throws Exception {
                MDC.put("destination", destination);
                ServerRunningData runningData = JsonUtils.unmarshalFromByte((byte[]) data, ServerRunningData.class);
                if (!isMine(runningData.getAddress())) {
                    mutex.set(false);
                }

                if (!runningData.isActive() && isMine(runningData.getAddress())) { // 说明出现了主动释放的操作，并且本机之前是active
                    releaseRunning();// 彻底释放mainstem
                }

                activeData = (ServerRunningData) runningData;
            }

            /**
             * 当其他canal instance出现异常，临时节点数据被删除时，会自动回调此方法，此时当前canal instance要顶上去
             */
            public void handleDataDeleted(String dataPath) throws Exception {
                MDC.put("destination", destination);
                mutex.set(false);
                if (!release && activeData != null && isMine(activeData.getAddress())) {
                    // 如果上一次active的状态就是本机，则即时触发一下active抢占
                    initRunning();
                } else {
                    // 否则就是等待delayTime，避免因网络瞬端或者zk异常，导致出现频繁的切换操作
                    delayExector.schedule(() -> initRunning(), delayTime, TimeUnit.SECONDS);
                }
            }

        };

    }

    public void init() {
        processStart();
    }

    /**
     * 方法被调用了，真正的启动 canal instance
     * 
     * 方法中涉及的 processStart 和 processActiveEnter 内部会调用 ServerRunningListener 的对应方法，
     * 在 CanalController 构造器中通过匿名内部类设置了该 Listener
     * 
     * 在 {@link com.alibaba.otter.canal.deployer.CanalController#start()} 被调用
     */
    public synchronized void start() {
        super.start();
        try {
            // 内部会调用 ServerRunningListener 的 processStart() 方法注册zk节点
            processStart();
            if (zkClient != null) {  // 如果 zkClient != null，就用 zk 进行 HA 启动
                String path = ZookeeperPathUtils.getDestinationServerRunning(destination);
                // 注册 destination 对应的 running 节点监听
                zkClient.subscribeDataChanges(path, dataListener);
                // 通过 HA 的方式来启动 CanalInstance
                initRunning();
            } else {
                // 内部调用 ServerRunningListener 的 processActiveEnter 方法
                processActiveEnter();// 没有zk，直接启动
            }
        } catch (Exception e) {
            logger.error("start failed", e);
            // 没有正常启动，重置一下状态，避免干扰下一次start
            stop();
        }

    }

    public boolean release() {
        if (zkClient != null) {
            releaseRunning(); // 尝试一下release
            return true;
        } else {
            processActiveExit(); // 没有zk，直接退出
            return false;
        }
    }

    public synchronized void stop() {
        super.stop();

        if (zkClient != null) {
            String path = ZookeeperPathUtils.getDestinationServerRunning(destination);
            zkClient.unsubscribeDataChanges(path, dataListener);

            releaseRunning(); // 尝试一下release
        } else {
            processActiveExit(); // 没有zk，直接启动
        }
        processStop();  // 释放zk节点
    }

    /**
     * 通过 HA 的方式来启动 CanalInstance，利用zk临时节点。
     * 
     * 在集群模式下，可能会有多个 canal server 共同处理同一个 destination，某一时刻只能由一个 canal server 进行处理，
     * 处理这个 destination 的 canal server 进入 running 状态，其他 canal server 进入 standby 状态。
     * 同时，通过监听对应的 path 节点，一旦发生变化，出现异常，可以立刻尝试自己进入 running，保证了 instance 的高可用。
     * 
     * 主要流程如下：
     * - 尝试创建zk临时节点：/otter/canal/destinations/{destination}/running，
     * - 如果节点已经存在，说明是其他的 canal server 已经启动了这个 canal instance。此时会抛出 ZkNodeExistsException，进入 catch 代码块。
     * - 如果创建成功，就说明没有其他 server 启动这个 instance，可以创建。
     */
    private void initRunning() {
        if (!isStart()) {
            return;
        }
        // 构建临时节点路径：/otter/canal/destinations/{destination}/running
        String path = ZookeeperPathUtils.getDestinationServerRunning(destination);
        // 序列化
        // 构建临时节点的数据，标记当前destination由哪一个canal server处理
        byte[] bytes = JsonUtils.marshalToByte(serverData);
        try {
            mutex.set(false);
            // 尝试创建临时节点。如果节点已经存在，说明是其他的canal server已经启动了这个canal instance。
            // 此时会抛出 ZkNodeExistsException，进入catch代码块。
            zkClient.create(path, bytes, CreateMode.EPHEMERAL);
            activeData = serverData;
            processActiveEnter();// 如果创建成功，触发一下事件，内部调用 ServerRunningListener 的 processActiveEnter 方法
            mutex.set(true);
            release = false;
        } catch (ZkNodeExistsException e) {
            // 节点已存在，则根据path从zk中获取当前是哪一个canal server创建了当前canal instance的相关信息。
            // 第二个参数true，表示的是，如果这个path不存在，则返回null。
            bytes = zkClient.readData(path, true);
            if (bytes == null) {// 如果不存在节点，立即尝试一次
                initRunning();
            } else {
                // 如果的确存在，则将创建该canal instance实例信息存入activeData中。
                activeData = JsonUtils.unmarshalFromByte(bytes, ServerRunningData.class);
            }
        } catch (ZkNoNodeException e) {
            // 父节点不存在，先创建父节点
            zkClient.createPersistent(ZookeeperPathUtils.getDestinationPath(destination), true); // 尝试创建父节点
            initRunning();
        }
    }

    /**
     * 阻塞等待自己成为active，如果自己成为active，立马返回
     * 
     * @throws InterruptedException
     */
    public void waitForActive() throws InterruptedException {
        initRunning();
        mutex.get();
    }

    /**
     * 检查当前的状态
     */
    public boolean check() {
        String path = ZookeeperPathUtils.getDestinationServerRunning(destination);
        try {
            byte[] bytes = zkClient.readData(path);
            ServerRunningData eventData = JsonUtils.unmarshalFromByte(bytes, ServerRunningData.class);
            activeData = eventData;// 更新下为最新值
            // 检查下nid是否为自己
            boolean result = isMine(activeData.getAddress());
            if (!result) {
                logger.warn("canal is running in node[{}] , but not in node[{}]",
                    activeData.getAddress(),
                    serverData.getAddress());
            }
            return result;
        } catch (ZkNoNodeException e) {
            logger.warn("canal is not run any in node");
            return false;
        } catch (ZkInterruptedException e) {
            logger.warn("canal check is interrupt");
            Thread.interrupted();// 清除interrupt标记
            return check();
        } catch (ZkException e) {
            logger.warn("canal check is failed");
            return false;
        }
    }

    private boolean releaseRunning() {
        if (check()) {
            release = true;
            String path = ZookeeperPathUtils.getDestinationServerRunning(destination);
            zkClient.delete(path);
            mutex.set(false);
            processActiveExit();
            return true;
        }

        return false;
    }

    // ====================== helper method ======================

    private boolean isMine(String address) {
        return address.equals(serverData.getAddress());
    }

    private void processStart() {
        if (listener != null) {
            try {
                listener.processStart();
            } catch (Exception e) {
                logger.error("processStart failed", e);
            }
        }
    }

    private void processStop() {
        if (listener != null) {
            try {
                listener.processStop();
            } catch (Exception e) {
                logger.error("processStop failed", e);
            }
        }
    }

    private void processActiveEnter() {
        if (listener != null) {
            listener.processActiveEnter();
        }
    }

    private void processActiveExit() {
        if (listener != null) {
            try {
                listener.processActiveExit();
            } catch (Exception e) {
                logger.error("processActiveExit failed", e);
            }
        }
    }

    public void setListener(ServerRunningListener listener) {
        this.listener = listener;
    }

    // ===================== setter / getter =======================

    public void setDelayTime(int delayTime) {
        this.delayTime = delayTime;
    }

    public void setServerData(ServerRunningData serverData) {
        this.serverData = serverData;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public void setZkClient(ZkClientx zkClient) {
        this.zkClient = zkClient;
    }

}
