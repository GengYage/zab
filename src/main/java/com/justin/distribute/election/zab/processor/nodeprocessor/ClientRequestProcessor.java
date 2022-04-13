package com.justin.distribute.election.zab.processor.nodeprocessor;

import com.justin.distribute.election.zab.NodeStatus;
import com.justin.distribute.election.zab.client.KVMessage;
import com.justin.distribute.election.zab.Node;
import com.justin.net.remoting.netty.NettyRequestProcessor;
import com.justin.net.remoting.protocol.RemotingMessage;
import io.netty.channel.ChannelHandlerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ClientRequestProcessor implements NettyRequestProcessor {
    private static final Logger logger = LogManager.getLogger(ClientRequestProcessor.class.getSimpleName());

    private final Node node;

    public ClientRequestProcessor(final Node node) {
        this.node = node;
    }

    /**
     * 处理客户端请求
     */
    @Override
    public RemotingMessage processRequest(ChannelHandlerContext ctx, RemotingMessage request) throws Exception {
        KVMessage kvMessage = KVMessage.getInstance().parseMessage(request);
        KVMessage.KVType kvType = kvMessage.getKvType();
        // 读请求，节点直接处理
        if (kvType == KVMessage.KVType.GET) {
            String value = node.getDataManager().get(kvMessage.getKey());
            if (value != null) {
                kvMessage.setValue(value);
            }else {
                kvMessage.setValue("");
            }
            kvMessage.setSuccess(true);

            logger.info("Client Get Response: " + kvMessage);
            return kvMessage.response(request);
        }

        // 写请求，转发给Leader
        if (node.getStatus() != NodeStatus.LEADING) {
            logger.info("Redirect to leader: " + node.getLeaderId());
            return node.redirect(request);
        }

        // 自己为Leader 则直接处理写请求
        if (kvType == KVMessage.KVType.PUT) {
            boolean flag = node.getDataManager().put(kvMessage.getKey(), kvMessage.getValue());
            if (flag) {
                // 现将写的消息设置未发起状态
                node.getSnapshotMap().put(node.getNodeConfig().getNodeId(), true);
                // 将消息广播至follower节点，使follower节点也将消息设置未发起状态
                node.appendData(kvMessage.getKey(), kvMessage.getValue());
                // 提交消息，是否能提交已在appendData阶段做了判断 countDownLatch
                boolean committed = node.commitData(kvMessage.getKey());
                // 设置写请求处理的结果
                kvMessage.setSuccess(committed);
            }else {
                kvMessage.setSuccess(false);
            }

            logger.info("Leader write log success!");
            return kvMessage.response(request);
        }
        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}
