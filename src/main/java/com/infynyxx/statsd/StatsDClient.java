package com.infynyxx.statsd;

import java.net.InetSocketAddress;
import java.util.Locale;
import java.util.Random;

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

    private final Bootstrap bootstrap;
    private final Channel channel;

    private final InetSocketAddress address;

    public StatsDClient(String host, int port) throws Exception{
        this(new InetSocketAddress(host, port));
    }

    public StatsDClient(InetSocketAddress inetSocketAddress) throws Exception {
        address = inetSocketAddress;
        bootstrap = new Bootstrap();
        bootstrap.group(new NioEventLoopGroup())
                .channel(new NioDatagramChannel())
                .remoteAddress(address)
                .option(ChannelOption.SO_BROADCAST, true)
                .handler(new StatsDClientInitializer());

        channel = bootstrap.connect().sync().channel();
    }

    public void shutdown() throws Exception{
        channel.closeFuture().sync();
        bootstrap.shutdown();
    }

    private ChannelFuture doSend(final String stat) {
        final DatagramPacket packet = new DatagramPacket(Unpooled.copiedBuffer(stat, CharsetUtil.UTF_8), address);
        final ChannelFuture future = channel.write(packet);
        return future;
    }

    private ChannelFuture send(double sampleRate, String stat) {
        if (sampleRate < 1.0) {
            if (RNG.nextDouble() <= sampleRate) {
                final String statString = String.format("%s|@%f", stat, sampleRate);
                return doSend(statString);
            }
        }
        return doSend(stat);
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
}

