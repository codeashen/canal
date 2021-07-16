package com.alibaba.otter.canal.store.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.alibaba.otter.canal.common.utils.CanalToStringStyle;
import com.alibaba.otter.canal.protocol.position.PositionRange;

/**
 * 代表一组数据对象的集合，类型是Event
 * {@link com.alibaba.otter.canal.store.model.Event}
 * 
 * @author jianghang 2012-6-14 下午09:07:41
 * @version 1.0.0
 */
public class Events<EVENT> implements Serializable {

    private static final long serialVersionUID = -7337454954300706044L;

    private PositionRange     positionRange    = new PositionRange();   // protocol模块类，描述这组数据的开始结束位置
    private List<EVENT>       events           = new ArrayList<>();     // 承载的一组数据（Event）

    public List<EVENT> getEvents() {
        return events;
    }

    public void setEvents(List<EVENT> events) {
        this.events = events;
    }

    public PositionRange getPositionRange() {
        return positionRange;
    }

    public void setPositionRange(PositionRange positionRange) {
        this.positionRange = positionRange;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, CanalToStringStyle.DEFAULT_STYLE);
    }
}
