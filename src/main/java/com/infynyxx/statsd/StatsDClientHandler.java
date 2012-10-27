package com.infynyxx.statsd;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundMessageHandlerAdapter;
import io.netty.channel.socket.DatagramPacket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Prajwal Tuladhar <praj@infynyxx.com>
 */
public class StatsDClientHandler extends ChannelInboundMessageHandlerAdapter<DatagramPacket> {

    private static final Logger logger = LoggerFactory.getLogger(StatsDClientHandler.class);

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.warn("Unexpected exception from downstream.");
        cause.printStackTrace();
    }

    @Override
    public void messageReceived(ChannelHandlerContext channelHandlerContext, DatagramPacket datagramPacket) throws Exception {
        logger.info("Received: " + datagramPacket.toString());
    }
}
