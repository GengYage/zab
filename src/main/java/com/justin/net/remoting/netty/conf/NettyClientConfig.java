package com.justin.net.remoting.netty.conf;


import lombok.Data;

@Data
public class NettyClientConfig {
    private int workerThreads = 4;
    private int callbackExecutorThreads = Runtime.getRuntime().availableProcessors();
    private int onewayValue = 65535;
    private int asyncValue = 65535;
    private int connectTimeout = 3000;
    private int channelMaxIdleSeconds = 120;
    private long channelNotActiveInterval = 1000 * 60;

    private int socketSndBufSize = 65535;
    private int socketRcvBufSize = 65535;
    private boolean closeSocketByClient = false;
}
