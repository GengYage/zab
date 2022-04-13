package com.justin.net.remoting;

import java.util.concurrent.ExecutorService;

public interface RemotingService {
    void start();
    void shutdown();
    void registerProcessor(final int requestCode, final RequestProcessor processor, final ExecutorService executor);

}
