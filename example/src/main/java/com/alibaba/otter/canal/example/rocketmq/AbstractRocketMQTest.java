package com.alibaba.otter.canal.example.rocketmq;

import com.alibaba.otter.canal.example.BaseCanalClientTest;

public abstract class AbstractRocketMQTest extends BaseCanalClientTest {

    public static String  topic              = "example";
    public static String  groupId            = "group";
    public static String  nameServers        = "127.0.0.1:9876";
    public static String  accessKey          = "";
    public static String  secretKey          = "";
    public static boolean enableMessageTrace = false;
    public static String  accessChannel      = "local";
    public static String  namespace          = "";
}
