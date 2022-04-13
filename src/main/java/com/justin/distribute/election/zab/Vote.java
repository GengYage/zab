package com.justin.distribute.election.zab;

import com.justin.distribute.election.zab.data.ZxId;


public class Vote implements Comparable<Vote> {
    private int nodeId;
    private volatile long epoch;
    private volatile int voteId;
    private volatile ZxId lastZxId;

    public Vote(final int nodeId, final int voteId, final long epoch, final ZxId lastZxId) {
        this.nodeId = nodeId;
        this.voteId = voteId;
        this.epoch = epoch;
        this.lastZxId = lastZxId;
    }

    public void incrementEpoch() {
        this.setEpoch(++epoch);
    }

    /**
     * 判断投票信息的有效性
     * 如果 两个zxid相同则比较 sid（nodeId）
     * 否则返回 zxid的比较结果
     * 若zxid 和 sid （nodeId）都想同，则返回0
     */
    @Override
    public int compareTo(Vote o) {
        if (this.lastZxId.compareTo(o.lastZxId) != 0) {
            return this.lastZxId.compareTo(o.lastZxId);
        } else if (this.nodeId < o.nodeId) {
            return -1;
        } else if (this.nodeId > o.nodeId) {
            return 1;
        }
        return 0;
    }

    @Override
    public String toString() {
        return "Vote: [" +
                " nodeId=" + nodeId +
                " voteId=" + voteId +
                " epoch=" + epoch +
                " lastZxId=" + lastZxId +
                "]";
    }

    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public ZxId getLastZxId() {
        return lastZxId;
    }

    public void setLastZxId(ZxId lastZxId) {
        this.lastZxId = lastZxId;
    }

    public int getVoteId() {
        return voteId;
    }

    public void setVoteId(int voteId) {
        this.voteId = voteId;
    }

    public long getEpoch() {
        return epoch;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }
}
