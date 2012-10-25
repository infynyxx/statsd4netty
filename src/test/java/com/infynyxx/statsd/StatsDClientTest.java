package com.infynyxx.statsd;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundMessageHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioEventLoopGroup;
import io.netty.util.CharsetUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StatsDClientTest {
    private static final int DUMMY_STATSD_SERVER_PORT = 9999;

    private DummyStatsDServer server = null;

    private static final ThreadFactory threadFactory = new ThreadFactoryBuilder().build();

    private static final Logger log = LoggerFactory.getLogger(StatsDClientTest.class);

    @Before
    public void setUp() throws InterruptedException {
        server = new DummyStatsDServer(DUMMY_STATSD_SERVER_PORT);
        Thread t = threadFactory.newThread(new Runnable() {
            @Override
            public void run() {
                try {
                    server.run();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        t.start();
    }

    @After
    public void tearDown() throws Exception {
        if (server != null) {
            log.info("Shutting down server");
            server.shutdown();
        }
    }

    @Test
    public void testSendWithDefaultIncrement() throws StatsDClientException, SocketException, InterruptedException {
        Thread.sleep(100L);
        StatsDClient statsDClient = new StatsDClient(new InetSocketAddress("localhost", DUMMY_STATSD_SERVER_PORT));
        final String metrics = "mailer.metric1";
        statsDClient.increment(metrics).sync();
        statsDClient.increment(metrics).sync();

        Thread.sleep(100L); // this is required since, it's hard to determine when the messages have arrived to Server queue

        assertEquals(2, server.getMessagesReceived().size());
        assertTrue(server.getMessagesReceived().contains(metrics + ":1|c"));

        statsDClient.shutdown();
    }


    public static final class DummyStatsDServer {
        private final int port;

        private static final Logger log = LoggerFactory.getLogger(DummyStatsDServer.class);

        private final DummyStatsServerHandler2 serverHandler;

        private final Bootstrap bootstrap;

        public DummyStatsDServer(int port) throws InterruptedException {
            this.port = port;
            this.serverHandler = new DummyStatsServerHandler2();
            bootstrap = new Bootstrap();
            bootstrap
                    .group(new NioEventLoopGroup())
                    .channel(NioDatagramChannel.class)
                    .localAddress(9999)
                    .option(ChannelOption.SO_BROADCAST, false)
                    .option(ChannelOption.SO_RCVBUF, 1024)
                    .handler(this.serverHandler);


        }

        public void run() throws InterruptedException {
            // Start the server.
            log.info("Starting Dummy StatsD Server on port: " + port);
            ChannelFuture f = bootstrap.bind().sync();
            f.channel().closeFuture().sync();
        }

        public List<String> getMessagesReceived() {
            return ImmutableList.copyOf(serverHandler.messagesReceived);
        }

        public void shutdown() {
            serverHandler.clearMessages();
            bootstrap.shutdown();
        }

        private class DummyStatsServerHandler2 extends ChannelInboundMessageHandlerAdapter<DatagramPacket> {

            private final Logger log = LoggerFactory.getLogger(DummyStatsServerHandler2.class);

            private final List<String> messagesReceived = new ArrayList<String>();

            @Override
            public void messageReceived(ChannelHandlerContext ctx, DatagramPacket msg) throws Exception {
                String message = msg.data().toString(CharsetUtil.UTF_8);
                log.info("Received: " + message);
                messagesReceived.add(message);
            }

            public synchronized void clearMessages() {
                messagesReceived.clear();
            }
        }
    }

}
