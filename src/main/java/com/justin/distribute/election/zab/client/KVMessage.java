package com.justin.distribute.election.zab.client;

import com.justin.distribute.election.zab.message.AbstractMessage;
import com.justin.distribute.election.zab.message.MessageType;

public class KVMessage extends AbstractMessage<KVMessage> {
    public enum KVType {
        PUT,
        GET,
    }

    private String key;
    private String value;
    private KVType kvType;

    private Boolean success;

    private KVMessage() {}

    public static KVMessage getInstance() {
        return new KVMessage();
    }

    @Override
    public int getMessageType() {
        return MessageType.CLIENT;
    }

    @Override
    public String toString() {
        return "KVMessage: [" +
                " key=" + key +
                " value=" + value +
                " kvType=" + kvType +
                " success=" + success +
                " ]";
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public KVType getKvType() {
        return kvType;
    }

    public void setKvType(KVType kvType) {
        this.kvType = kvType;
    }

    public Boolean getSuccess() {
        return success;
    }

    public void setSuccess(Boolean success) {
        this.success = success;
    }
}
