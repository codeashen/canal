package com.alibaba.otter.canal.server;

import com.alibaba.otter.canal.common.CanalLifeCycle;
import com.alibaba.otter.canal.server.exception.CanalServerException;

/**
 * 对应canal整个服务实例，一个jvm实例只有一份server
 * 
 * canalServer 支持两种模式，CanalServerWithEmbedded 和 CanalServerWithNetty。
 * 二者都实现了 CanalServer 接口，且都实现了单例模式，通过静态方法 instance 获取实例。
 * 
 * 官方文档描述：https://github.com/alibaba/canal/wiki/%E7%AE%80%E4%BB%8B#server%E8%AE%BE%E8%AE%A1
 * 
 * @author jianghang 2012-7-12 下午01:32:29
 * @version 1.0.0
 */
public interface CanalServer extends CanalLifeCycle {

    void start() throws CanalServerException;

    void stop() throws CanalServerException;
}
