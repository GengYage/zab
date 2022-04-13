package com.justin.net.remoting.netty;

import com.justin.net.remoting.RequestProcessor;
import com.justin.net.remoting.protocol.RemotingMessage;
import io.netty.channel.ChannelHandlerContext;

public interface NettyRequestProcessor extends RequestProcessor {
    RemotingMessage processRequest(ChannelHandlerContext ctx, RemotingMessage request) throws Exception;
}
