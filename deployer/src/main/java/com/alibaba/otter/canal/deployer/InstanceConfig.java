package com.alibaba.otter.canal.deployer;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.alibaba.otter.canal.common.utils.CanalToStringStyle;

/**
 * 启动的相关配置
 * 表示canal instance的全局配置，承载 canal.properties 中 canal.instance.global.* 配置
 * 
 * @author jianghang 2012-11-8 下午02:50:54
 * @version 1.0.0
 */
public class InstanceConfig {

    /**
     * 根据全局配置生成单个instance可以使用逐个属性赋值的方式，但是作者采用这种维护一个全局配置引用的方式，
     * 这种聚合的方式，让所有的属性get方法也都做了适配，更好的体现了全局和个体配置的关系
     */
    private InstanceConfig globalConfig;    // 每个 InstanceConfig 对象中都维护一个全局的配置对象
    private InstanceMode   mode;            // canal instance 配置加载方式，取值有 manager|spring 两种方式
    private Boolean        lazy;            // canal instance 是否延迟初始化
    private String         managerAddress;  // 配置中心地址，远程加载时需要
    private String         springXml;       // spring 配置文件路径

    public InstanceConfig(){

    }

    public InstanceConfig(InstanceConfig globalConfig){
        this.globalConfig = globalConfig;
    }

    public static enum InstanceMode {
        SPRING, MANAGER;

        public boolean isSpring() {
            return this == InstanceMode.SPRING;
        }

        public boolean isManager() {
            return this == InstanceMode.MANAGER;
        }
    }

    public Boolean getLazy() {
        if (lazy == null && globalConfig != null) {
            return globalConfig.getLazy();
        } else {
            return lazy;
        }
    }

    public void setLazy(Boolean lazy) {
        this.lazy = lazy;
    }

    public InstanceMode getMode() {
        if (mode == null && globalConfig != null) {
            return globalConfig.getMode();
        } else {
            return mode;
        }
    }

    public void setMode(InstanceMode mode) {
        this.mode = mode;
    }

    public String getManagerAddress() {
        if (managerAddress == null && globalConfig != null) {
            return globalConfig.getManagerAddress();
        } else {
            return managerAddress;
        }
    }

    public void setManagerAddress(String managerAddress) {
        this.managerAddress = managerAddress;
    }

    public String getSpringXml() {
        if (springXml == null && globalConfig != null) {
            return globalConfig.getSpringXml();
        } else {
            return springXml;
        }
    }

    public void setSpringXml(String springXml) {
        this.springXml = springXml;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, CanalToStringStyle.DEFAULT_STYLE);
    }

}
