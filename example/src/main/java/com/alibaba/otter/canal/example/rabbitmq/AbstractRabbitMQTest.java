package com.alibaba.otter.canal.example.rabbitmq;

import com.alibaba.otter.canal.example.BaseCanalClientTest;

public abstract class AbstractRabbitMQTest extends BaseCanalClientTest {
    
    public static String  nameServer         = "qamq-cc.intra.yeshj.com";
    public static int     port               = 5672;
    public static String  vhost              = "ccwebqa";
    public static String  queueName          = "canal.test.topic.q";
    public static String  username           = "ccwebqa";
    public static String  password           = "ccwebqa";
    public static String  accessKey          = "";
    public static String  secretKey          = "";
    public static Long  resourceOwnerId      = 0L;
}
