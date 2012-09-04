package com.infynyxx.statsd;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundMessageHandlerAdapter;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.CharsetUtil;

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
        channelHandlerContext.write(new DatagramPacket(Unpooled.copiedBuffer("hello", CharsetUtil.UTF_8), datagramPacket.remoteAddress()));
        logger.info(datagramPacket.toString());
    }
}
