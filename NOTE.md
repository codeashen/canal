# Canal测试笔记

## 一、TCP方式直接消费

1. 修改instance配置 
   
   deployer 模块 example/instance.propertis
   
   ```pro
   ## mysql serverId
   canal.instance.mysql.slaveId = 1234
   #position info，需要改成自己的数据库信息
   canal.instance.master.address = 192.168.58.128:3306
   canal.instance.master.journal.name =
   canal.instance.master.position =
   canal.instance.master.timestamp =
   #canal.instance.standby.address = 
   #canal.instance.standby.journal.name =
   #canal.instance.standby.position = 
   #canal.instance.standby.timestamp = 
   #username/password，需要改成自己的数据库信息
   canal.instance.dbUsername = canal
   canal.instance.dbPassword = canal
   canal.instance.defaultDatabaseName =
   canal.instance.connectionCharset = UTF-8
   #table regex
   canal.instance.filter.regex = .\*\\\\..\*
   ```
   
   配置文件指定了源数据库连接信息
   
   > 为了方便本地调试，可以修改 `logback.xml` 将日志输出到控制台
   
2. 启动 CanalLauncher

3. 启动 example 模块 SimpleCanalClientTest 类，按情况修改 ip 地址

4. 修改源数据库的数据，可以在测试类控制台输出的信息

## 二、adapter通过TCP方式消费

### 2.1 Logger简单消费示例

1. 修改 client-adapter/launch 模块配置 `application.yml`

   > 为了避免配置混乱，可以直接复制一份 `application-logger.yml`，启动的时间修改 `spring.profiles.active`

   ```pro
   server:
     port: 8081
   logging:
     level:
       com.alibaba.otter.canal.client.adapter.hbase: DEBUG
   spring:
     jackson:
       date-format: yyyy-MM-dd HH:mm:ss
       time-zone: GMT+8
       default-property-inclusion: non_null
   
   canal.conf:
     canalServerHost: 192.168.58.128:11111
     batchSize: 500
     syncBatchSize: 1000
     retries: 0
     timeout:
     mode: tcp
     consumerProperties:
       # canal tcp consumer
       canal.tcp.server.host: 192.168.58.128:11111
       canal.tcp.batch.size: 500
       canal.tcp.username: canal
       canal.tcp.password: canal
     canalAdapters:
       - instance: example
         groups:
           - groupId: g1
             outerAdapters:
               - name: logger       
   
   #  srcDataSources:
   #    defaultDS:
   #      url: jdbc:mysql://127.0.0.1:3306/mytest?useUnicode=true
   #      username: root
   #      password: 121212
   ```

2. 运行 SpringBoot 启动类 `CanalAdapterApplication`

3. 源数据库修改数据表，控制台观察

### 2.2 消费同步至es7

1. 安装es和kibana

2. 源数据库创建数据库和数据表

   ```sql
   create database mytest;
   use mytest;
   CREATE TABLE `user` (
     `id` int(10) NOT NULL,
     `name` varchar(100) DEFAULT NULL,
     `role_id` int(10) NOT NULL,
     `c_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
     `c_utime` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
     PRIMARY KEY (`id`)
   ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
   ```

3. 创建es索引

   ```json
   PUT /mytest_user
   {
     "mappings": {
       "_doc": {
         "properties": {
           "name": {
             "type": "text",
             "fields": {
               "keyword": {
                 "type": "keyword"
               }
             }
           },
           "role_id": {
             "type": "long"
           },
           "c_time": {
             "type": "date"
           }
         }
       }
     }
   }
   ```

   > es7创建索引 mapping 中不需要 "_doc" 了

4. client-adapter/launcher 复制一份 `application-es7.yml`

   ```yml
   server:
     port: 8081
   logging:
     level:
       com.alibaba.otter.canal.client.adapter.hbase: DEBUG
   spring:
     jackson:
       date-format: yyyy-MM-dd HH:mm:ss
       time-zone: GMT+8
       default-property-inclusion: non_null
   
   canal.conf:
     canalServerHost: 192.168.58.128:11111     # 对应单机模式下的canal server的ip:port
   #  zookeeperHosts: slave1:2181               # 对应集群模式下的zk地址, 如果配置了canalServerHost, 则以canalServerHost为准
   #  mqServers: qamq-cc.intra.yeshj.com:5672   # MQ地址, 与canalServerHost不能并存
     flatMessage: true                         # 扁平message开关, 是否以json字符串形式投递数据, 仅在kafka/rocketMQ模式下有效
     batchSize: 500
     syncBatchSize: 1000
     retries: 0
     timeout:
     mode: tcp
     srcDataSources:
       defaultDS:
         url: jdbc:mysql://192.168.58.128:3306/mytest?useUnicode=true&useSSL=false
         username: root
         password: root
     consumerProperties:
       # canal tcp consumer
       canal.tcp.server.host: 192.168.58.128:11111
       canal.tcp.batch.size: 500
       canal.tcp.username: canal
       canal.tcp.password: canal
     canalAdapters:
       - instance: example                       # canal 实例名或者 MQ topic 名, RabbitMQ 下代表 Queue 名
         groups:
           - groupId: g1
             outerAdapters:
               - name: logger
               - name: es7                       # or es6
                 key: es7Key
                 hosts: 127.0.0.1:9300           # 127.0.0.1:9200 for rest mode
                 properties:
                   mode: transport               # 可指定transport模式或者rest模式
                   # security.auth: test:123456  #  only used for rest mode
                   cluster.name: my-application  # es cluster name
   ```

5. 修改 client-adapter/es7x 模块配置 `mytest_user.yml`

   ```yml
   dataSourceKey: defaultDS        # 源数据源的key, 对应上面配置的srcDataSources中的值
   outerAdapterKey: es7Key         # 对应application.yml中es配置的key 
   destination: example            # cannal的instance或者MQ的topic
   groupId: g1                     # 对应MQ模式下的groupId, 只会同步对应groupId的数据
   esMapping:
     _index: mytest_user           # es 的索引名称
     _id: _id                      # es 的_id, 如果不配置该项必须配置下面的pk项_id则会由es自动分配
   #  upsert: true
   #  pk: id                       # 如果不需要_id, 则需要指定一个属性为主键属性
     # sql映射
     sql: "select a.id as _id, a.name, a.role_id, a.c_time from user a"
   #  objFields:
   #    _labels: array:;           # 数组或者对象属性, array:; 代表以;字段里面是以;分隔的
   #    _obj: object               # json对象
     etlCondition: "where a.c_time>='{0}'"     # etl 的条件参数
     commitBatch: 3000                         # 提交批大小
   ```

   就是在 adapter 的 group 下添加一个 outerAdapters，指定为 es7，配置一些信息

   有关 canalAdapter、group、outerAdapter 之间的关系，参考：[官方文档-adapter定义配置部分](https://github.com/alibaba/canal/wiki/ClientAdapter#21-%E6%80%BB%E9%85%8D%E7%BD%AE%E6%96%87%E4%BB%B6-applicationyml)

   sql映射配置了源数据库中表和es中索引的对应关系，关于映射关系，参考：[官方文档-适配器映射文件](https://github.com/alibaba/canal/wiki/Sync-ES#2-%E9%80%82%E9%85%8D%E5%99%A8%E8%A1%A8%E6%98%A0%E5%B0%84%E6%96%87%E4%BB%B6)

6. 运行 adapter launcher 启动类 `CanalAdapterApplication`

7. 修改源数据库，查看控制台，查询es索引数据

### 问题

1. 运行 launcher 时会，报 ClassCastException，Druid.DataSource 转换问题

   escore模块和common模块中druid依赖冲突问题，将escore模块的pom文件中druid依赖加 `<scope>provided</scope>`

2. 报错 `Config dir not found.`

   launcher 的 target/classes 下没有 es7 文件夹，手动将 `es7x/resources/es7` 拷贝过去，即可启动，但是为什么 install 的时候没有编译进去？

3. 普遍问题，本机调试的时候，修改数据，只能收到 transactionBegin 和 transactionEnd 的消息，收不到 dml，连接虚拟机中的 canalServer 和 mysql 就好了

## 三、RabbitMQ消费

官方版本没有支持 RabbitMQ，有人提出pr，被官方吸收。起始已经支持 RabbitMQ，官方文档没更新而已，参考：[issues-2156](https://github.com/alibaba/canal/pull/2156)

以下步骤省略了 RabbitMQ搭建，直接用本地或测试环境的都可以

1. canalServer（本地 canal.deployer），添加主配置 `canal.properties`

   ```properties
   rabbitmq.host = qamq-cc.intra.yeshj.com:5672
   rabbitmq.virtual.host = ccwebqa
   rabbitmq.exchange = canal.test.topic.ex
   rabbitmq.username = ccwebqa
   rabbitmq.password = ccwebqa
   ```

2. canalServer（本地 canal.deployer），添加 instance 配置 `example/instance.properties`

   ```properties
   # kafka/rocketmq中表示topic，rabbitmq中表示routing key
   canal.mq.topic=canal.test.routingKey
   ```

3. 修改 client-adapter 中 launcher 配置

   ```yml
   server:
     port: 8081
   logging:
     level:
       com.alibaba.otter.canal.client.adapter.hbase: DEBUG
   spring:
     jackson:
       date-format: yyyy-MM-dd HH:mm:ss
       time-zone: GMT+8
       default-property-inclusion: non_null
   
   canal.conf:
   #  canalServerHost: 192.168.58.128:11111     # 对应单机模式下的canal server的ip:port
   #  zookeeperHosts: slave1:2181               # 对应集群模式下的zk地址, 如果配置了canalServerHost, 则以canalServerHost为准
     mqServers: qamq-cc.intra.yeshj.com:5672   # kafka或rocketMQ地址, 与canalServerHost不能并存
     flatMessage: true                         # 扁平message开关, 是否以json字符串形式投递数据, 仅在kafka/rocketMQ模式下有效
     batchSize: 500
     syncBatchSize: 1000
     retries: 0
     timeout:
     mode: rabbitMQ
     srcDataSources:
       defaultDS:
         url: jdbc:mysql://192.168.58.128:3306/mytest?useUnicode=true&useSSL=false
         username: root
         password: root
     consumerProperties:
       # canal tcp consumer
       canal.tcp.server.host: 192.168.58.128:11111
       canal.tcp.batch.size: 500
       canal.tcp.username: canal
       canal.tcp.password: canal
       # rabbitMQ consumer
       rabbitmq.host: qamq-cc.intra.yeshj.com:5672
       rabbitmq.virtual.host: ccwebqa
       rabbitmq.username: ccwebqa
       rabbitmq.password: ccwebqa
       rabbitmq.resource.ownerId:
     canalAdapters:
       - instance: canal.test.topic.q            # canal 实例名或者 MQ topic 名, RabbitMQ 下代表 Queue 名
         groups:
           - groupId: g1
             outerAdapters:
               - name: logger
               - name: es7                       # or es6
                 key: es7Key
                 hosts: 127.0.0.1:9300           # 127.0.0.1:9200 for rest mode
                 properties:
                   mode: transport               # 可指定transport模式或者rest模式
                   # security.auth: test:123456  #  only used for rest mode
                   cluster.name: my-application  # es cluster name
   ```

   - 修改了 `canal.conf.mode` 为 rabbitMQ
   - 添加了rabbitmq的配置
   - 修改了 `canalAdapter` 中 `instance` 的名称，tcp模式下表示实例名，mq模式下表示topic或队列名

4. 重新启动 adapter，修改源数据库，验证控制台和es数据



