package com.justin.distribute.election.zab.data;

import com.justin.net.remoting.common.Pair;


public class Data {
    private ZxId zxId;
    private Pair<String, String> kv;

    public Data() {}

    public Data(final ZxId zxId, final Pair<String, String> kv) {
        this.zxId = zxId;
        this.kv = kv;
    }

    @Override
    public String toString() {
        return "Data: {" +
                " zxId=" + zxId +
                " kv=[" + kv.getObject1() + ":" + kv.getObject2() +
                "}";
    }

    public ZxId getZxId() {
        return zxId;
    }

    public void setZxId(ZxId zxId) {
        this.zxId = zxId;
    }

    public Pair<String, String> getKv() {
        return kv;
    }

    public void setKv(Pair<String, String> kv) {
        this.kv = kv;
    }
}
