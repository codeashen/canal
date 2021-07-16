package com.alibaba.otter.canal.server;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.alibaba.otter.canal.connector.core.config.MQProperties;
import com.alibaba.otter.canal.connector.core.producer.MQDestination;
import com.alibaba.otter.canal.connector.core.spi.CanalMQProducer;
import com.alibaba.otter.canal.connector.core.util.Callback;
import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.instance.core.CanalMQConfig;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.server.embedded.CanalServerWithEmbedded;

public class CanalMQStarter {

    private static final Logger          logger         = LoggerFactory.getLogger(CanalMQStarter.class);

    private volatile boolean             running        = false;

    // 工作线程池，对每个 instance 起一个 worker 线程
    private ExecutorService              executorService;

    // 消息生产者，投递 mq 消息
    private CanalMQProducer              canalMQProducer;

    // 封装 MQ 配置
    private MQProperties                 mqProperties;

    // 委托给 CanalServerWithEmbedded 处理，和 CanalServerWithNetty 一样
    private CanalServerWithEmbedded      canalServer;

    // 记录了 destination(instance的标识) 和 worker 线程的关系
    private Map<String, CanalMQRunnable> canalMQWorks   = new ConcurrentHashMap<>();

    private static Thread                shutdownThread = null;

    public CanalMQStarter(CanalMQProducer canalMQProducer){
        this.canalMQProducer = canalMQProducer;
    }

    public synchronized void start(String destinations) {
        try {
            if (running) {
                return;
            }
            mqProperties = canalMQProducer.getMqProperties();
            // set filterTransactionEntry
            if (mqProperties.isFilterTransactionEntry()) {
                System.setProperty("canal.instance.filter.transaction.entry", "true");
            }

            // 1、获取单例的 CanalServerWithEmbedded
            canalServer = CanalServerWithEmbedded.instance();

            // region 2、对应每个 instance 启动一个 worker 线程 CanalMQRunnable
            executorService = Executors.newCachedThreadPool();
            logger.info("## start the MQ workers.");
            
            String[] dsts = StringUtils.split(destinations, ",");
            for (String destination : dsts) {
                destination = destination.trim();
                // 每个 instance 对应的 worker 线程，丢到线程池里去执行
                CanalMQRunnable canalMQRunnable = new CanalMQRunnable(destination);
                canalMQWorks.put(destination, canalMQRunnable);
                executorService.execute(canalMQRunnable);
            }

            running = true;  // 设置启动标识位，线程run方法
            logger.info("## the MQ workers is running now ......");
            // endregion

            // region 3、注册 ShutdownHook，退出时关闭线程池和 mqProducer
            shutdownThread = new Thread(() -> {
                try {
                    logger.info("## stop the MQ workers");
                    running = false;
                    executorService.shutdown();
                    canalMQProducer.stop();
                } catch (Throwable e) {
                    logger.warn("##something goes wrong when stopping MQ workers:", e);
                } finally {
                    logger.info("## canal MQ is down.");
                }
            });
            Runtime.getRuntime().addShutdownHook(shutdownThread);
            // endregion
            
        } catch (Throwable e) {
            logger.error("## Something goes wrong when starting up the canal MQ workers:", e);
        }
    }

    public synchronized void destroy() {
        running = false;
        if (executorService != null) {
            executorService.shutdown();
        }
        if (canalMQProducer != null) {
            canalMQProducer.stop();
        }
        if (shutdownThread != null) {
            Runtime.getRuntime().removeShutdownHook(shutdownThread);
            shutdownThread = null;
        }
    }

    /**
     * 为指定 destination 注册一个工作线程，放到线程池中执行
     * @param destination
     */
    public synchronized void startDestination(String destination) {
        CanalInstance canalInstance = canalServer.getCanalInstances().get(destination);
        if (canalInstance != null) {
            stopDestination(destination);
            CanalMQRunnable canalMQRunnable = new CanalMQRunnable(destination);
            canalMQWorks.put(canalInstance.getDestination(), canalMQRunnable);
            executorService.execute(canalMQRunnable);
            logger.info("## Start the MQ work of destination:" + destination);
        }
    }

    /**
     * 停止并移除指定 destination 的工作线程
     * @param destination
     */
    public synchronized void stopDestination(String destination) {
        CanalMQRunnable canalMQRunable = canalMQWorks.get(destination);
        if (canalMQRunable != null) {
            canalMQRunable.stop();
            canalMQWorks.remove(destination);
            logger.info("## Stop the MQ work of destination:" + destination);
        }
    }

    /**
     * 工作线程方法，每一个工作线程对应一个 instance（destination名指定的），
     * 用于循环拉取 binlog，发送 MQ
     * @param destination
     * @param destinationRunning
     */
    private void worker(String destination, AtomicBoolean destinationRunning) {
        while (!running || !destinationRunning.get()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // ignore
            }
        }
        logger.info("## start the MQ producer: {}.", destination);
        MDC.put("destination", destination);
        
        // 1、给自己创建一个身份标识，作为 client
        final ClientIdentity clientIdentity = new ClientIdentity(destination, (short) 1001, "");
        while (running && destinationRunning.get()) {
            try {
                // 2、根据 destination 获取对应 instance，如果没有就 sleep，等待产生（比如从别的 server 那边 HA 过来一个 instance）
                CanalInstance canalInstance = canalServer.getCanalInstances().get(destination);
                if (canalInstance == null) {
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                    continue;
                }
                
                // 3、构建一个 MQ 的 destination 对象, 加载相关 mq 的配置信息，用作 mqProducer 的入参
                MQDestination canalDestination = new MQDestination();
                canalDestination.setCanalDestination(destination);
                CanalMQConfig mqConfig = canalInstance.getMqConfig();
                canalDestination.setTopic(mqConfig.getTopic());
                canalDestination.setPartition(mqConfig.getPartition());
                canalDestination.setDynamicTopic(mqConfig.getDynamicTopic());
                canalDestination.setPartitionsNum(mqConfig.getPartitionsNum());
                canalDestination.setPartitionHash(mqConfig.getPartitionHash());
                canalDestination.setDynamicTopicPartitionNum(mqConfig.getDynamicTopicPartitionNum());

                // 4、在 embeddedCanal 中注册这个订阅客户端
                canalServer.subscribe(clientIdentity);
                logger.info("## the MQ producer: {} is running now ......", destination);

                Integer getTimeout = mqProperties.getFetchTimeout();
                Integer getBatchSize = mqProperties.getBatchSize();
                // 5、开始运行，并通过 embededCanal 进行流式 get/ack/rollback 协议，进行数据消费
                while (running && destinationRunning.get()) {
                    Message message;
                    // 5.1 getWithoutAck获取message
                    if (getTimeout != null && getTimeout > 0) {
                        message = canalServer.getWithoutAck(clientIdentity,
                            getBatchSize,
                            getTimeout.longValue(),
                            TimeUnit.MILLISECONDS);
                    } else {
                        message = canalServer.getWithoutAck(clientIdentity, getBatchSize);
                    }

                    final long batchId = message.getId();
                    try {
                        int size = message.isRaw() ? message.getRawEntries().size() : message.getEntries().size();
                        if (batchId != -1 && size != 0) {
                            // 5.2 通过mqProducer发送消息，投递成功commit，失败rollback
                            canalMQProducer.send(canalDestination, message, new Callback() {
                                @Override
                                public void commit() {
                                    canalServer.ack(clientIdentity, batchId); // 提交确认
                                }

                                @Override
                                public void rollback() {
                                    canalServer.rollback(clientIdentity, batchId);
                                }
                            }); // 发送message到topic
                        } else {
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException e) {
                                // ignore
                            }
                        }
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                }
            } catch (Exception e) {
                logger.error("process error!", e);
            }
        }
    }

    /**
     * MQ工作线程的Runnable
     */
    private class CanalMQRunnable implements Runnable {

        private String destination;

        CanalMQRunnable(String destination){
            this.destination = destination;
        }

        private AtomicBoolean running = new AtomicBoolean(true);

        @Override
        public void run() {
            worker(destination, running);  // 具体工作
        }

        public void stop() {
            running.set(false);
        }
    }
}
