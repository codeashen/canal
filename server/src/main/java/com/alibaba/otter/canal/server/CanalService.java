package com.alibaba.otter.canal.server;

import java.util.concurrent.TimeUnit;

import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.server.exception.CanalServerException;

/**
 * 定义客户端不同请求的处理接口，只有 CanalServerWithEmbedded 一个实现
 * 
 * 每个方法的入参都带有 ClientIdentity，这个是客户端的身份标识
 */
public interface CanalService {
    /**
     * 客户端订阅，重复订阅时会更新对应的filter信息
     */
    void subscribe(ClientIdentity clientIdentity) throws CanalServerException;

    /**
     * 取消订阅
     */
    void unsubscribe(ClientIdentity clientIdentity) throws CanalServerException;

    /**
     * 比例获取数据，并自动自行ack
     */
    Message get(ClientIdentity clientIdentity, int batchSize) throws CanalServerException;

    /**
     * 超时时间内批量获取数据，并自动进行ack
     */
    Message get(ClientIdentity clientIdentity, int batchSize, Long timeout, TimeUnit unit) throws CanalServerException;

    /**
     * 批量获取数据，不进行ack
     */
    Message getWithoutAck(ClientIdentity clientIdentity, int batchSize) throws CanalServerException;

    /**
     * 超时时间内批量获取数据，不进行ack
     */
    Message getWithoutAck(ClientIdentity clientIdentity, int batchSize, Long timeout, TimeUnit unit) throws CanalServerException;

    /**
     * ack某个批次的数据
     */
    void ack(ClientIdentity clientIdentity, long batchId) throws CanalServerException;

    /**
     * 回滚所有没有ack的批次的数据
     */
    void rollback(ClientIdentity clientIdentity) throws CanalServerException;

    /**
     * 回滚某个批次的数据
     */
    void rollback(ClientIdentity clientIdentity, Long batchId) throws CanalServerException;
}
