package com.infynyxx.statsd;

import java.net.InetSocketAddress;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioEventLoopGroup;
import io.netty.util.CharsetUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Prajwal Tuladhar <praj@infynyxx.com>
 */

public class StatsDClient {
    private static Random RNG = new Random();
    private static final Logger log = LoggerFactory.getLogger(StatsDClient.class);

    private static final ThreadFactory THREAD_FACTORY = new ThreadFactoryBuilder()
            .setNameFormat("statsd-java-client-%d")
            .build();

    private final Bootstrap bootstrap;
    private final Channel channel;

    private final InetSocketAddress address;

    private String metricPrefix = "";

    public StatsDClient(String host, int port) throws Exception{
        this(new InetSocketAddress(host, port));
    }

    public StatsDClient(InetSocketAddress inetSocketAddress) throws Exception {
        address = inetSocketAddress;
        bootstrap = new Bootstrap();
        bootstrap.group(configureNioEventLoopGroup())
                .channel(NioDatagramChannel.class)
                .remoteAddress(address)
                .option(ChannelOption.SO_BROADCAST, true)
                .handler(new StatsDClientInitializer());

        channel = bootstrap.connect().sync().channel();
    }

    private static NioEventLoopGroup configureNioEventLoopGroup() {
        return new NioEventLoopGroup(0, THREAD_FACTORY); // if nThreads = 0 means, Netty will use 2 X Available Cores threads
    }


    /**
     * Shutdown datagram channel
     * @throws StatsDClientException
     */
    public void shutdown() throws StatsDClientException {
        log.info("Shutting down StatsD Client");
        try {
            channel.closeFuture().sync();
            bootstrap.shutdown();
        } catch (Exception e) {
            throw new StatsDClientException(e);
        }
    }

    /**
     * Shutdown datagram channel while waiting to be completed within the specified time limit and also releasing ChannelFactory resources
     * This method should be called when shutting down the application
     * @param timeToWait
     * @param unit
     * @throws StatsDClientException
     */
    public void shutdown(long timeToWait, TimeUnit unit) throws StatsDClientException {
        log.info("Shutting down StatsD Client");
        try {
            channel.close().sync().await(timeToWait, unit);
        } catch (InterruptedException e) {
            throw new StatsDClientException(e);
        }
    }

    private ChannelFuture doSend(final String stat) {
        final DatagramPacket packet = new DatagramPacket(Unpooled.copiedBuffer(stat, CharsetUtil.UTF_8), address);
        final ChannelFuture future = channel.write(packet);
        return future;
    }

    private ChannelFuture send(double sampleRate, String stat) {
        final String finalStat = this.metricPrefix + stat;
        if (sampleRate < 1.0) {
            if (RNG.nextDouble() <= sampleRate) {
                final String statString = String.format("%s|@%f", finalStat, sampleRate);
                return doSend(statString);
            }
        }
        return doSend(finalStat);
    }

    public ChannelFuture increment(String key) {
        return increment(key, 1, 1.0);
    }

    public ChannelFuture increment(String key, int magnitude) {
        return increment(key, magnitude, 1.0);
    }

    public ChannelFuture increment(String key, int magnitude, double sampleRate) {
        String stat = String.format(Locale.ENGLISH, "%s:%s|c", key, magnitude);
        return send(sampleRate, stat);
    }

    public ChannelFuture timing(String key, int timeInMs, double sampleRate) {
        final String stat = String.format(Locale.ENGLISH, "%s:%d|ms", key, timeInMs);
        return send(sampleRate, stat);
    }

    public ChannelFuture timing(String key, int timeInMs) {
        final String stat = String.format(Locale.ENGLISH, "%s:%d|ms", key, timeInMs);
        return send(1.0, stat);
    }

    public ChannelFuture gauge(String key, int value) {
        final String stat = String.format(Locale.ENGLISH, "%s:%d|g", key, value);
        return send(1.0, stat);
    }

    /**
     * Set base metric prefix
     * @param prefix
     */
    public void setMetricPrefix(final String prefix) {
        this.metricPrefix = prefix + ".";
    }
}

