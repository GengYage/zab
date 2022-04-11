package com.justin.distribute.election.zab.message.nodemsg;

import com.justin.distribute.election.zab.data.Data;
import com.justin.distribute.election.zab.message.AbstractMessage;
import com.justin.distribute.election.zab.message.MessageType;


public class DataMessage extends AbstractMessage<DataMessage> {
    public enum Type {
        SYNC, SNAPSHOT, COMMIT
    }

    private int nodeId;
    private Data data;
    private Type type;

    private Boolean success;

    private DataMessage() {}

    public static DataMessage getInstance() {
        return new DataMessage();
    }

    @Override
    public int getMessageType() {
        return MessageType.DATA_SYNC;
    }

    @Override
    public String toString() {
        return " DataMessage: [" +
                " nodeId=" + nodeId +
                " data=" + data +
                " type=" + type +
                " success=" + success +
                "]";
    }

    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public Data getData() {
        return data;
    }

    public void setData(Data data) {
        this.data = data;
    }

    public Boolean getSuccess() {
        return success;
    }

    public void setSuccess(Boolean success) {
        this.success = success;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }
}
