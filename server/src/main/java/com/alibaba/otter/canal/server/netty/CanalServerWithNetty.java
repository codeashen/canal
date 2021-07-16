package com.alibaba.otter.canal.server.netty;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.server.CanalServer;
import com.alibaba.otter.canal.server.embedded.CanalServerWithEmbedded;
import com.alibaba.otter.canal.server.netty.handler.ClientAuthenticationHandler;
import com.alibaba.otter.canal.server.netty.handler.FixedHeaderFrameDecoder;
import com.alibaba.otter.canal.server.netty.handler.HandshakeInitializationHandler;
import com.alibaba.otter.canal.server.netty.handler.SessionHandler;

/**
 * 基于netty网络服务的server实现
 * 
 * 独立部署一个 canal server，使用 canal 提供的客户端，连接 canal server 获取 binlog 解析后数据。
 * 而 CanalServerWithNetty 是在 CanalServerWithEmbedded 的基础上做的一层封装，用于与客户端通信。
 * 
 * 在独立部署 canal server 时，Canal 客户端发送的所有请求都交给 CanalServerWithNetty 处理解析，
 * 解析完成之后委派给了交给 CanalServerWithEmbedded 进行处理。因此 CanalServerWithNetty 就是一个马甲而已。
 * CanalServerWithEmbedded 才是核心。
 * 
 * 因此，在构造器中，我们看到，用于生成 CanalInstance 实例的 instanceGenerator 被设置到了 CanalServerWithEmbedded 中，
 * 而 ip 和 port 被设置到 CanalServerWithNetty 中。
 * 
 * @author jianghang 2012-7-12 下午01:34:49
 * @version 1.0.0
 */
public class CanalServerWithNetty extends AbstractCanalLifeCycle implements CanalServer {

    // 监听的所有客户端请求都会委派给CanalServerWithEmbedded处理 
    private CanalServerWithEmbedded embeddedServer;      // 嵌入式server
    // netty监听的网络ip和端口，client 通过这个 ip 和端口与 server 通信
    private String                  ip;
    private int                     port;
    // netty组件
    private Channel                 serverChannel = null;
    private ServerBootstrap         bootstrap     = null;
    private ChannelGroup            childGroups   = null; // socket channel container, used to close sockets explicitly.

    private static class SingletonHolder {

        private static final CanalServerWithNetty CANAL_SERVER_WITH_NETTY = new CanalServerWithNetty();
    }

    private CanalServerWithNetty(){
        this.embeddedServer = CanalServerWithEmbedded.instance();
        this.childGroups = new DefaultChannelGroup();
    }

    public static CanalServerWithNetty instance() {
        return SingletonHolder.CANAL_SERVER_WITH_NETTY;
    }

    public void start() {
        super.start();
        
        // region 1、优先启动内嵌的canal server，因为基于netty的实现需要将请求委派给其处理
        if (!embeddedServer.isStart()) {
            embeddedServer.start();
        }
        // endregion

        // region 2、初始化并设置bootstrap实例
        this.bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool()));
        /*
         * enable keep-alive mechanism, handle abnormal network connection
         * scenarios on OS level. the threshold parameters are depended on OS.
         * e.g. On Linux: net.ipv4.tcp_keepalive_time = 300
         * net.ipv4.tcp_keepalive_probes = 2 net.ipv4.tcp_keepalive_intvl = 30
         */
        bootstrap.setOption("child.keepAlive", true);
        /*
         * optional parameter.
         */
        bootstrap.setOption("child.tcpNoDelay", true);
        // endregion

        // region 3、构造对应的pipeline，设置netty对客户端请求的处理器链
        bootstrap.setPipelineFactory(() -> {
            ChannelPipeline pipelines = Channels.pipeline();
            // 3.1 主要是处理编码、解码。因为网路传输的传入的都是二进制流，FixedHeaderFrameDecoder的作用就是对其进行解析
            pipelines.addLast(FixedHeaderFrameDecoder.class.getName(), new FixedHeaderFrameDecoder());
            // support to maintain child socket channel.
            // 3.2 处理client与server握手
            pipelines.addLast(HandshakeInitializationHandler.class.getName(),
                new HandshakeInitializationHandler(childGroups));
            // 3.3 client身份验证
            pipelines.addLast(ClientAuthenticationHandler.class.getName(),
                new ClientAuthenticationHandler(embeddedServer));
            // 3.4 ============= SessionHandler用于真正的处理客户端请求 =============
            // 处理逻辑都在这里面，构造中传入embeddedServer，因为请求都要委派给embeddedServer处理
            SessionHandler sessionHandler = new SessionHandler(embeddedServer);
            pipelines.addLast(SessionHandler.class.getName(), sessionHandler);
            return pipelines;
        });
        // endregion

        // region 4、启动netty服务器，开始真正的监控某个端口，此时客户端对这个端口的请求可以被接受到
        if (StringUtils.isNotEmpty(ip)) {
            this.serverChannel = bootstrap.bind(new InetSocketAddress(this.ip, this.port));
        } else {
            this.serverChannel = bootstrap.bind(new InetSocketAddress(this.port));
        }
        // endregion
    }

    public void stop() {
        super.stop();

        if (this.serverChannel != null) {
            this.serverChannel.close().awaitUninterruptibly(1000);
        }

        // close sockets explicitly to reduce socket channel hung in complicated
        // network environment.
        if (this.childGroups != null) {
            this.childGroups.close().awaitUninterruptibly(5000);
        }

        if (this.bootstrap != null) {
            this.bootstrap.releaseExternalResources();
        }

        if (embeddedServer.isStart()) {
            embeddedServer.stop();
        }
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setEmbeddedServer(CanalServerWithEmbedded embeddedServer) {
        this.embeddedServer = embeddedServer;
    }

}
