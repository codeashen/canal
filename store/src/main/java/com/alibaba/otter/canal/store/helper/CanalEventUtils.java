package com.alibaba.otter.canal.store.helper;

import org.apache.commons.lang.StringUtils;

import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.alibaba.otter.canal.store.model.Event;

/**
 * 相关的操作工具
 * 
 * @author jianghang 2012-6-19 下午05:49:21
 * @version 1.0.0
 */
public class CanalEventUtils {

    /**
     * 找出一个最小的position位置，相等的情况返回position1
     */
    public static LogPosition min(LogPosition position1, LogPosition position2) {
        if (position1.getIdentity().equals(position2.getIdentity())) {
            // 首先根据文件进行比较
            if (position1.getPostion().getJournalName().compareTo(position2.getPostion().getJournalName()) > 0) {
                return position2;
            } else if (position1.getPostion().getJournalName().compareTo(position2.getPostion().getJournalName()) < 0) {
                return position1;
            } else {
                // 根据offest进行比较
                if (position1.getPostion().getPosition() > position2.getPostion().getPosition()) {
                    return position2;
                } else {
                    return position1;
                }
            }
        } else {
            // 不同的主备库，根据时间进行比较
            if (position1.getPostion().getTimestamp() > position2.getPostion().getTimestamp()) {
                return position2;
            } else {
                return position1;
            }
        }
    }

    /**
     * 根据entry创建对应的Position对象
     * 事实上，parser 模块解析后，已经将位置信息：binlog 文件，position 封装到了 Event 中，createPosition 方法只是将这些信息提取出来
     */
    public static LogPosition createPosition(Event event) {
        // =============创建一个EntryPosition实例，提取event中的位置信息============
        EntryPosition position = new EntryPosition();
        // event所在的binlog文件
        position.setJournalName(event.getJournalName());
        // event所在binlog文件中的位置
        position.setPosition(event.getPosition());
        // event的创建时间
        position.setTimestamp(event.getExecuteTime());
        // event是mysql主从集群哪一个实例上生成的，一般都是主库，如果从库没有配置read-only，那么serverId也可能是从库
        position.setServerId(event.getServerId());
        // add gtid
        position.setGtid(event.getGtid());
        // ===========将EntryPosition实例封装到一个LogPosition对象中===============
        LogPosition logPosition = new LogPosition();
        logPosition.setPostion(position);
        // LogIdentity中包含了这个event来源的mysql实例的ip地址信息
        logPosition.setIdentity(event.getLogIdentity());
        return logPosition;
    }

    /**
     * 根据entry创建对应的Position对象
     */
    public static LogPosition createPosition(Event event, boolean included) {
        EntryPosition position = new EntryPosition();
        position.setJournalName(event.getJournalName());
        position.setPosition(event.getPosition());
        position.setTimestamp(event.getExecuteTime());
        position.setIncluded(included);
        // add serverId at 2016-06-28
        position.setServerId(event.getServerId());
        // add gtid
        position.setGtid(event.getGtid());

        LogPosition logPosition = new LogPosition();
        logPosition.setPostion(position);
        logPosition.setIdentity(event.getLogIdentity());
        return logPosition;
    }

    /**
     * 判断当前的entry和position是否相同
     */
    public static boolean checkPosition(Event event, LogPosition logPosition) {
        EntryPosition position = logPosition.getPostion();
        boolean result = position.getTimestamp().equals(event.getExecuteTime());

        boolean exactely = (StringUtils.isBlank(position.getJournalName()) && position.getPosition() == null);
        if (!exactely) {// 精确匹配
            result &= position.getPosition().equals(event.getPosition());
            if (result) {// short path
                result &= StringUtils.equals(event.getJournalName(), position.getJournalName());
            }
        }

        return result;
    }
}
