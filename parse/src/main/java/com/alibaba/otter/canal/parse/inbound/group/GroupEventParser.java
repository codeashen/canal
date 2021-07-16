package com.alibaba.otter.canal.parse.inbound.group;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.parse.CanalEventParser;

/**
 * 伪装成多个 mysql 实例的 slave 解析 binglog 日志。
 * 内部维护了多个 CanalEventParser，组合多个 EventParser 进行合并处理，group 只是作为一个 delegate 处理。
 * 
 * 主要应用场景是分库分表：比如一个大表拆分了 4 个库，位于不同的 mysql 实例上，正常情况下，我们需要配置四个 CanalInstance。
 * 对应的，业务上要消费数据时，需要启动 4 个客户端，分别链接 4 个 instance 实例。
 * 
 * 为了方便业务使用，此时我们可以让 CanalInstance 引用一个 GroupEventParser，
 * 由 GroupEventParser 内部维护 4 个 MysqlEventParser 去 4 个不同的 mysql 实例去拉取 binlog，最终合并到一起。
 * 此时业务只需要启动 1 个客户端，链接这个 CanalInstance 即可
 * 
 * @author jianghang 2012-10-16 上午11:23:14
 * @version 1.0.0
 */
public class GroupEventParser extends AbstractCanalLifeCycle implements CanalEventParser {

    private List<CanalEventParser> eventParsers = new ArrayList<>();

    public void start() {
        super.start();
        // 统一启动
        for (CanalEventParser eventParser : eventParsers) {
            if (!eventParser.isStart()) {
                eventParser.start();
            }
        }
    }

    public void stop() {
        super.stop();
        // 统一关闭
        for (CanalEventParser eventParser : eventParsers) {
            if (eventParser.isStart()) {
                eventParser.stop();
            }
        }
    }

    public void setEventParsers(List<CanalEventParser> eventParsers) {
        this.eventParsers = eventParsers;
    }

    public void addEventParser(CanalEventParser eventParser) {
        if (!eventParsers.contains(eventParser)) {
            eventParsers.add(eventParser);
        }
    }

    public void removeEventParser(CanalEventParser eventParser) {
        eventParsers.remove(eventParser);
    }

    public List<CanalEventParser> getEventParsers() {
        return eventParsers;
    }

}
