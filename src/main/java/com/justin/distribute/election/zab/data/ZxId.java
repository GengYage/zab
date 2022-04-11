package com.justin.distribute.election.zab.data;


public class ZxId implements Comparable<ZxId> {
    private long epoch;
    private long counter;

    public ZxId() {}

    public ZxId(final long epoch, final long counter) {
        this.epoch = epoch;
        this.counter = counter;
    }

    @Override
    public int compareTo(ZxId o) {
        if (this.epoch < o.epoch) {
            return -1;
        }else if (this.epoch > o.epoch) {
            return 1;
        }else if (this.counter < o.counter) {
            return -1;
        }else if (this.counter > o.counter) {
            return 1;
        }
        return 0;
    }

    @Override
    public String toString() {
        return "ZxId: [" +
                " epoch=" + epoch +
                " counter=" + counter +
                "]";
    }

    public long getEpoch() {
        return epoch;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    public long getCounter() {
        return counter;
    }

    public void setCounter(long counter) {
        this.counter = counter;
    }
}
