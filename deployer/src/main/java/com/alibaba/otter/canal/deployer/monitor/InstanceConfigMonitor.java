package com.alibaba.otter.canal.deployer.monitor;

import com.alibaba.otter.canal.common.CanalLifeCycle;

/**
 * 监听instance file的文件变化，触发instance start/stop等操作
 * 
 * @author jianghang 2013-2-6 下午06:19:56
 * @version 1.0.1
 */
public interface InstanceConfigMonitor extends CanalLifeCycle {

    /**
     * 注册对指定destination的监听
     * @param destination 指定的destination
     * @param action      配置发生变化的默认行为
     */
    void register(String destination, InstanceAction action);

    /**
     * 取消对指定destination的监听，根本没被调用过
     * @param destination 指定的destination
     */
    void unregister(String destination);
}
