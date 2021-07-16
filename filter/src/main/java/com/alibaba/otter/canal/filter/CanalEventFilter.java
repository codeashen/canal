package com.alibaba.otter.canal.filter;

import com.alibaba.otter.canal.filter.exception.CanalFilterException;

/**
 * 数据过滤机制
 * 提供了三种实现，实际上只用到了AviaterRegexFilter，在canal的Spring配置文件中可以看到，
 * eventParser中有两个Filter，分别用于白名单和黑名单过滤，类型都是AviaterRegexFilter
 * 
 * @author jianghang 2012-7-20 下午03:51:27
 */
public interface CanalEventFilter<T> {

    boolean filter(T event) throws CanalFilterException;
}
