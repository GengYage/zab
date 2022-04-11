package com.justin.distribute.election.zab.message;

import com.justin.net.remoting.protocol.JSONSerializable;
import com.justin.net.remoting.protocol.RemotingMessage;
import com.justin.net.remoting.protocol.RemotingMessageHeader;


public abstract class AbstractMessage<T> {

    public RemotingMessage request() {
        RemotingMessageHeader header = new RemotingMessageHeader();
        header.setCode(getMessageType());

        byte[] body = JSONSerializable.encode(this);
        return new RemotingMessage(header, body);
    }

    public RemotingMessage response(final RemotingMessage request) {
        byte[] body = JSONSerializable.encode(this);

        return new RemotingMessage(request.getMessageHeader(), body);
    }

    public T parseMessage(final RemotingMessage remotingMessage) {
        return (T) JSONSerializable.decode(remotingMessage.getMessageBody(), this.getClass());
    }

    public abstract int getMessageType();
}
