package com.alibaba.otter.canal.common.zookeeper.running;

import java.util.Map;

/**
 * {@linkplain ServerRunningMonitor}管理容器，使用static进行数据全局共享
 * 
 * ServerRunningMonitor 用于监控 CanalInstance
 * 
 * 在 CanalController 的构造器中，canal 会为每一个 destination 创建一个 Instance，
 * 每个 Instance 都会由一个 ServerRunningMonitor 来进行控制。
 * 而 ServerRunningMonitor 统一由 ServerRunningMonitors 进行管理。
 * 
 * 除了CanalInstance需要监控，CanalServer本身也需要监控，所以还维护了ServerRunningData对象，
 * 封装了canal server监听的ip和端口等信息。
 * 
 * @author jianghang 2012-12-3 下午09:32:06
 * @version 1.0.0
 */
public class ServerRunningMonitors {

    private static ServerRunningData serverData;
    private static Map               runningMonitors; // <String, ServerRunningMonitor>

    public static ServerRunningData getServerData() {
        return serverData;
    }

    public static Map<String, ServerRunningMonitor> getRunningMonitors() {
        return runningMonitors;
    }

    public static ServerRunningMonitor getRunningMonitor(String destination) {
        return (ServerRunningMonitor) runningMonitors.get(destination);
    }

    public static void setServerData(ServerRunningData serverData) {
        ServerRunningMonitors.serverData = serverData;
    }

    public static void setRunningMonitors(Map runningMonitors) {
        ServerRunningMonitors.runningMonitors = runningMonitors;
    }

}
