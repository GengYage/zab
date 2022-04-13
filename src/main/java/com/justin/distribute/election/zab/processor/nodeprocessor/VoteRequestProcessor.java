package com.justin.distribute.election.zab.processor.nodeprocessor;

import com.justin.distribute.election.zab.Node;
import com.justin.distribute.election.zab.NodeStatus;
import com.justin.distribute.election.zab.Vote;
import com.justin.distribute.election.zab.message.nodemsg.VoteMessage;
import com.justin.net.remoting.netty.NettyRequestProcessor;
import com.justin.net.remoting.protocol.RemotingMessage;
import io.netty.channel.ChannelHandlerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.TimeUnit;


public class VoteRequestProcessor implements NettyRequestProcessor {
    private static final Logger logger = LogManager.getLogger(VoteRequestProcessor.class.getSimpleName());

    private final Node node;

    public VoteRequestProcessor(final Node node) {
        this.node = node;
    }

    // 处理收到的投票
    @Override
    public RemotingMessage processRequest(ChannelHandlerContext ctx, RemotingMessage request) throws Exception {
        try {
            if (node.getVoteLock().tryLock(3000, TimeUnit.MILLISECONDS)) {
                VoteMessage resVoteMsg = VoteMessage.getInstance().parseMessage(request);
                Vote peerVote = resVoteMsg.getVote();
                logger.info("Receive peer vote: {}", peerVote);
                // 将收到的投票保存到本地
                node.getVoteBox().put(peerVote.getNodeId(), peerVote);
                // 收到的投票的epoch大于自身(投票者比自己更适合成为Leader), 则更新自身投票信息
                if (peerVote.getEpoch() > node.getMyVote().getEpoch()) {
                    node.getMyVote().setEpoch(peerVote.getEpoch());
                    node.getMyVote().setVoteId(peerVote.getNodeId());
                    node.setStatus(NodeStatus.LOOKING);
                } else if (peerVote.getEpoch() == node.getMyVote().getEpoch()) {
                    // 比较投票有效性，若投票者比自己更适合成为Leader则更新自己的投票消息
                    if (peerVote.compareTo(node.getMyVote()) > 0) {
                        node.getMyVote().setVoteId(peerVote.getNodeId());
                        node.setStatus(NodeStatus.LOOKING);
                    }
                }
                // 判断自己获得票数，若过半则节点成为Leader更新自身状态
                if (node.isHalf()) {
                    logger.info("Node:{} become leader!", node.getNodeConfig().getNodeId());
                    // 宣告自己成为Leader
                    node.becomeLeader();
                } else if (node.getStatus() == NodeStatus.LOOKING) {
                    // 向集群中其他节点宣告自己的投票信息
                    VoteMessage voteMsg = VoteMessage.getInstance();
                    voteMsg.setVote(node.getMyVote());
                    node.sendOneWayMsg(voteMsg.request());
                }
            }
            return null;
        } finally {
            node.getVoteLock().unlock();
        }
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}
