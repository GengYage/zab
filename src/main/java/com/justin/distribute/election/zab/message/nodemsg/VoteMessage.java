package com.justin.distribute.election.zab.message.nodemsg;

import com.justin.distribute.election.zab.Vote;
import com.justin.distribute.election.zab.message.AbstractMessage;
import com.justin.distribute.election.zab.message.MessageType;

public class VoteMessage extends AbstractMessage<VoteMessage> {
    private Vote vote;

    private Boolean success;

    private VoteMessage() {}

    public static VoteMessage getInstance() {
        return new VoteMessage();
    }

    @Override
    public int getMessageType() {
        return MessageType.VOTE;
    }

    @Override
    public String toString() {
        return "VoteMessage: [" +
                " vote=" + vote +
                " success=" + success +
                "]";
    }

    public Vote getVote() {
        return vote;
    }

    public void setVote(Vote vote) {
        this.vote = vote;
    }

    public Boolean getSuccess() {
        return success;
    }

    public void setSuccess(Boolean success) {
        this.success = success;
    }
}
