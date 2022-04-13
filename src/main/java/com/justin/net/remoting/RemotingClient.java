package com.justin.net.remoting;

import com.justin.net.remoting.protocol.RemotingMessage;

import java.util.concurrent.ExecutorService;


public interface RemotingClient extends RemotingService {
    RemotingMessage invokeSync(final String addr, final RemotingMessage request, final long timeout) throws Exception;
    void invokeAsync(final String addr, final RemotingMessage request, final long timeout, final InvokeCallback invokeCallback) throws Exception;
    void invokeOneway(final String addr, final RemotingMessage request, final long timeout) throws Exception;

    void setCallbackExecutor(final ExecutorService executor);
    ExecutorService getCallbackExecutor();
}


