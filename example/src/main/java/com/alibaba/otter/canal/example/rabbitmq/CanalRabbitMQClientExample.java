package com.alibaba.otter.canal.example.rabbitmq;

import com.alibaba.otter.canal.client.rabbitmq.RabbitMQCanalConnector;
import com.alibaba.otter.canal.protocol.FlatMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * RabbitMQ client example
 *
 * @author wanggaosheng @ 2021-6-1
 * @version 1.0.0
 */
public class CanalRabbitMQClientExample extends AbstractRabbitMQTest {

    protected final static Logger           logger  = LoggerFactory.getLogger(CanalRabbitMQClientExample.class);

    private RabbitMQCanalConnector connector;

    private static volatile boolean         running = false;

    private Thread                          thread  = null;

    private Thread.UncaughtExceptionHandler handler = (t, e) -> logger.error("parse events has an error", e);

    public CanalRabbitMQClientExample(String nameServer, int port, String vhost, String queueName, String accessKey, 
                                      String secretKey, String username, String password, Long resourceOwnerId) {
        connector = new RabbitMQCanalConnector(nameServer, port, vhost, queueName, accessKey, 
                secretKey, username, password, resourceOwnerId, true);
    }

    public static void main(String[] args) {
        try {
            final CanalRabbitMQClientExample rabbitMQClientExample = new CanalRabbitMQClientExample(nameServer, 
                    port,
                    vhost, 
                    queueName, 
                    accessKey, 
                    secretKey, 
                    username, 
                    password, 
                    resourceOwnerId);
            logger.info("## Start the rabbitmq consumer: {}-{}", vhost, queueName);
            rabbitMQClientExample.start();
            logger.info("## The canal rabbitmq consumer is running now ......");
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    logger.info("## Stop the rabbitmq consumer");
                    rabbitMQClientExample.stop();
                } catch (Throwable e) {
                    logger.warn("## Something goes wrong when stopping rabbitmq consumer:", e);
                } finally {
                    logger.info("## Rabbitmq consumer is down.");
                }
            }));
            while (running)
                ;
        } catch (Throwable e) {
            logger.error("## Something going wrong when starting up the rabbitmq consumer:", e);
            System.exit(0);
        }
    }

    public void start() {
        Assert.notNull(connector, "connector is null");
        thread = new Thread(this::process);
        thread.setUncaughtExceptionHandler(handler);
        thread.start();
        running = true;
    }

    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    private void process() {
        while (!running) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }

        while (running) {
            try {
                connector.connect();
                connector.subscribe();
                while (running) {
                    List<FlatMessage> messages = connector.getFlatListWithoutAck(1000L, TimeUnit.MILLISECONDS); // 获取message
                    for (FlatMessage message : messages) {
                        long batchId = message.getId();
                        if (batchId == -1 || message.getData() == null) {
                            // try {
                            // Thread.sleep(1000);
                            // } catch (InterruptedException e) {
                            // }
                        } else {
                            logger.info("Received a message from rabbitmq: \n{}", message.toString());
                        }
                    }

                    connector.ack(); // 提交确认
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        connector.unsubscribe();
        // connector.stopRunning();
    }
}
