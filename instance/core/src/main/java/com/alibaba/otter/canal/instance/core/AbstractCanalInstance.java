package com.alibaba.otter.canal.instance.core;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.common.alarm.CanalAlarmHandler;
import com.alibaba.otter.canal.filter.aviater.AviaterRegexFilter;
import com.alibaba.otter.canal.meta.CanalMetaManager;
import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.parse.ha.CanalHAController;
import com.alibaba.otter.canal.parse.ha.HeartBeatHAController;
import com.alibaba.otter.canal.parse.inbound.AbstractEventParser;
import com.alibaba.otter.canal.parse.inbound.group.GroupEventParser;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlEventParser;
import com.alibaba.otter.canal.parse.index.CanalLogPositionManager;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.sink.CanalEventSink;
import com.alibaba.otter.canal.store.CanalEventStore;
import com.alibaba.otter.canal.store.model.Event;

/**
 * CanalInstance实例上层父类，封装了实例工作的所有组件。
 * 
 * AbstractCanalInstance 各属性的初始化是在实现类中完成的，有manager和spring两个实现类。
 * 但是核心工作逻辑都定义在本抽象类中定义，实现类仅仅做属性的初始化。
 * 
 * AbstractCanalInstance 除了负责启动和停止其内部组件，就没有其他工作了。
 * eventParser 在 AbstractCanalInstance 中启动后，就会自行开启多线程任务 dump 数据，通过 eventSink 投递给 eventStore。
 * 而对 eventStore 的操作逻辑，实际上都是在 CanalServerWithEmbedded 中完成的，参考 getWithoutAck() 的相关逻辑。
 * 
 * 所以，其实这里只是简单的启动和停止，组件的交互逻辑是在 CanalServerWithEmbedded 中 get 出 instance 的各个组件来进行实现的。
 * 
 * Created with Intellij IDEA. Author: yinxiu Date: 2016-01-07 Time: 22:26
 */
public class AbstractCanalInstance extends AbstractCanalLifeCycle implements CanalInstance {

    private static final Logger                      logger = LoggerFactory.getLogger(AbstractCanalInstance.class);

    protected Long                                   canalId;       // 和manager交互唯一标示
    protected String                                 destination;   // 队列名字
    protected CanalEventStore<Event>                 eventStore;    // 有序队列，数据存储
    protected CanalEventParser                       eventParser;   // 解析对应的数据信息，数据源接入，模拟slave协议和master进行交互，协议解析
    protected CanalEventSink<List<CanalEntry.Entry>> eventSink;     // 链接parse和store的桥接器
    protected CanalMetaManager                       metaManager;   // 消费信息管理器，增量订阅/消费binlog元数据位置存储
    protected CanalAlarmHandler                      alarmHandler;  // alarm报警机制
    protected CanalMQConfig                          mqConfig;      // mq的配置


    /**
     * 订阅关系发生变化时，做的操作，主要是更新一下filter，即订阅哪些库表
     * @param identity 客户端身份
     * @return
     */
    @Override
    public boolean subscribeChange(ClientIdentity identity) {
        if (StringUtils.isNotEmpty(identity.getFilter())) {// 如果设置了filter
            logger.info("subscribe filter change to " + identity.getFilter());
            // 1、根据客户端指定的filter表达式，创建filter对象
            AviaterRegexFilter aviaterFilter = new AviaterRegexFilter(identity.getFilter());

            // 2、更新filter
            boolean isGroup = (eventParser instanceof GroupEventParser);
            if (isGroup) {
                // 2.1 如果是group的模式，逐一更新filter
                List<CanalEventParser> eventParsers = ((GroupEventParser) eventParser).getEventParsers();
                for (CanalEventParser singleEventParser : eventParsers) {// 需要遍历启动
                    if(singleEventParser instanceof AbstractEventParser) {
                        ((AbstractEventParser) singleEventParser).setEventFilter(aviaterFilter);
                    }
                }
            } else {
                // 2.2 非group模式，直接更新filter
                if(eventParser instanceof AbstractEventParser) {
                    ((AbstractEventParser) eventParser).setEventFilter(aviaterFilter);
                }
            }
        }

        // filter的处理规则
        // a. parser处理数据过滤处理
        // b. sink处理数据的路由&分发,一份parse数据经过sink后可以分发为多份，每份的数据可以根据自己的过滤规则不同而有不同的数据
        // 后续内存版的一对多分发，可以考虑
        
        // filter 规定了需要订阅哪些库，哪些表。在服务端和客户端都可以设置，客户端的配置会覆盖服务端的配置
        // 服务端配置：主要是配置 instance.properties 中的 canal.instance.filter.regex 配置项
        // 客户端在订阅时，调用 CanalConnector 接口中定义的带有 filter 参数的 subscribe 方法重载形式
        return true;
    }

    /**
     * instance启动执行的方法，按照依赖顺序，
     * 顺序为 metaManager -> alarmHandler -> eventStore -> eventSink -> eventParser。
     * 官方关于instance模块构成的图中，把metaManager放在最下面，说明其是最基础的部分，因此应该最先启动。
     * 而eventParser依赖于eventSink，需要把自己解析的binlog交给其加工过滤，而eventSink又要把处理后的数据交给eventStore进行存储。
     */
    @Override
    public void start() {
        super.start();
        if (!metaManager.isStart()) {
            metaManager.start();
        }

        if (!alarmHandler.isStart()) {
            alarmHandler.start();
        }

        if (!eventStore.isStart()) {
            eventStore.start();
        }

        if (!eventSink.isStart()) {
            eventSink.start();
        }

        if (!eventParser.isStart()) {
            beforeStartEventParser(eventParser);  // 启动前执行一些操作
            eventParser.start();
            afterStartEventParser(eventParser);   // 启动后执行一些操作
        }
        logger.info("start successful....");
    }

    /**
     * instance停止方法，顺序和start相反
     */
    @Override
    public void stop() {
        super.stop();
        logger.info("stop CannalInstance for {}-{} ", new Object[] { canalId, destination });

        if (eventParser.isStart()) {
            beforeStopEventParser(eventParser); // 停止前执行一些操作
            eventParser.stop();
            afterStopEventParser(eventParser);  // 停止后执行一些操作
        }

        if (eventSink.isStart()) {
            eventSink.stop();
        }

        if (eventStore.isStart()) {
            eventStore.stop();
        }

        if (metaManager.isStart()) {
            metaManager.stop();
        }

        if (alarmHandler.isStart()) {
            alarmHandler.stop();
        }

        logger.info("stop successful....");
    }

    protected void beforeStartEventParser(CanalEventParser eventParser) {
        // 1、判断eventParser的类型是否是GroupEventParser
        boolean isGroup = (eventParser instanceof GroupEventParser);
        // 2、如果是GroupEventParser，则循环启动其内部包含的每一个CanalEventParser，依次调用startEventParserInternal方法
        if (isGroup) {
            // 处理group的模式
            List<CanalEventParser> eventParsers = ((GroupEventParser) eventParser).getEventParsers();
            for (CanalEventParser singleEventParser : eventParsers) {// 需要遍历启动
                startEventParserInternal(singleEventParser, true);
            }
        } else {
            // 如果不是，说明是一个普通的CanalEventParser，直接调用startEventParserInternal方法   
            startEventParserInternal(eventParser, false);
        }
    }

    /**
     * 通过 metaManager 读取一下历史订阅过这个 CanalInstance 的客户端信息，然后更新一下 filter
     * @param eventParser
     */
    // around event parser, default impl
    protected void afterStartEventParser(CanalEventParser eventParser) {
        // 读取一下历史订阅的filter信息
        List<ClientIdentity> clientIdentitys = metaManager.listAllSubscribeInfo(destination);
        for (ClientIdentity clientIdentity : clientIdentitys) {
            //更新filter
            subscribeChange(clientIdentity);
        }
    }

    // around event parser
    protected void beforeStopEventParser(CanalEventParser eventParser) {
        // noop
    }

    protected void afterStopEventParser(CanalEventParser eventParser) {

        boolean isGroup = (eventParser instanceof GroupEventParser);
        if (isGroup) {
            // 处理group的模式
            List<CanalEventParser> eventParsers = ((GroupEventParser) eventParser).getEventParsers();
            for (CanalEventParser singleEventParser : eventParsers) {// 需要遍历启动
                stopEventParserInternal(singleEventParser);
            }
        } else {
            stopEventParserInternal(eventParser);
        }
    }

    /**
     * 初始化单个eventParser，不需要考虑group<p>
     *
     * 每个 EventParser 都会关联两个内部组件
     * <ul>
     * <li> CanalLogPositionManager: 记录 binlog 最后一次解析成功位置信息，主要是描述下一次 canal 启动的位点 </li>
     * <li> CanalHAController: 控制 EventParser 的链接主机管理，判断当前该链接哪个 mysql 数据库 </li>
     * </ul>
     */
    protected void startEventParserInternal(CanalEventParser eventParser, boolean isGroup) {
        // 1 、启动CanalLogPositionManager
        if (eventParser instanceof AbstractEventParser) {
            AbstractEventParser abstractEventParser = (AbstractEventParser) eventParser;
            // 首先启动log position管理器
            CanalLogPositionManager logPositionManager = abstractEventParser.getLogPositionManager();
            if (!logPositionManager.isStart()) {
                logPositionManager.start();
            }
        }

        // 2 、启动CanalHAController
        if (eventParser instanceof MysqlEventParser) {
            MysqlEventParser mysqlEventParser = (MysqlEventParser) eventParser;
            CanalHAController haController = mysqlEventParser.getHaController();

            if (haController instanceof HeartBeatHAController) {
                ((HeartBeatHAController) haController).setCanalHASwitchable(mysqlEventParser);
            }

            if (!haController.isStart()) {
                haController.start();
            }

        }
    }

    protected void stopEventParserInternal(CanalEventParser eventParser) {
        if (eventParser instanceof AbstractEventParser) {
            AbstractEventParser abstractEventParser = (AbstractEventParser) eventParser;
            // 首先启动log position管理器
            CanalLogPositionManager logPositionManager = abstractEventParser.getLogPositionManager();
            if (logPositionManager.isStart()) {
                logPositionManager.stop();
            }
        }

        if (eventParser instanceof MysqlEventParser) {
            MysqlEventParser mysqlEventParser = (MysqlEventParser) eventParser;
            CanalHAController haController = mysqlEventParser.getHaController();
            if (haController.isStart()) {
                haController.stop();
            }
        }
    }

    // ==================getter==================================
    @Override
    public String getDestination() {
        return destination;
    }

    @Override
    public CanalEventParser getEventParser() {
        return eventParser;
    }

    @Override
    public CanalEventSink getEventSink() {
        return eventSink;
    }

    @Override
    public CanalEventStore getEventStore() {
        return eventStore;
    }

    @Override
    public CanalMetaManager getMetaManager() {
        return metaManager;
    }

    @Override
    public CanalAlarmHandler getAlarmHandler() {
        return alarmHandler;
    }

    @Override
    public CanalMQConfig getMqConfig() {
        return mqConfig;
    }
}
