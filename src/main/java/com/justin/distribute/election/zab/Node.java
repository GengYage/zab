package com.justin.distribute.election.zab;

import com.justin.distribute.election.zab.data.Data;
import com.justin.distribute.election.zab.data.DataManager;
import com.justin.distribute.election.zab.data.ZxId;
import com.justin.distribute.election.zab.message.MessageType;
import com.justin.distribute.election.zab.message.nodemgrmsg.JoinGroupMessage;
import com.justin.distribute.election.zab.message.nodemsg.DataMessage;
import com.justin.distribute.election.zab.message.nodemsg.VoteMessage;
import com.justin.distribute.election.zab.processor.nodemgrprocessor.JoinGroupProcessor;
import com.justin.distribute.election.zab.processor.nodeprocessor.ClientRequestProcessor;
import com.justin.distribute.election.zab.processor.nodeprocessor.DataRequestProcessor;
import com.justin.distribute.election.zab.processor.nodeprocessor.VoteRequestProcessor;
import com.justin.net.remoting.RemotingClient;
import com.justin.net.remoting.RemotingServer;
import com.justin.net.remoting.common.Pair;
import com.justin.net.remoting.netty.NettyRemotingClient;
import com.justin.net.remoting.netty.NettyRemotingServer;
import com.justin.net.remoting.netty.conf.NettyClientConfig;
import com.justin.net.remoting.netty.conf.NettyServerConfig;
import com.justin.net.remoting.protocol.RemotingMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class Node {
    private static final Logger logger = LogManager.getLogger(Node.class.getSimpleName());

    private final ConcurrentMap<Integer, Vote> voteBox = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, ZxId> zxIdMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, Boolean> snapshotMap = new ConcurrentHashMap<>();
    private final Lock voteLock = new ReentrantLock();
    private final Lock dataLock = new ReentrantLock();
    private final CountDownLatch countDownLatch = new CountDownLatch(1);

    private final ExecutorService executorService;
    private final ScheduledExecutorService scheduledExecutorService;
    private final NodeConfig nodeConfig;
    private final DataManager dataManager;

    private volatile NodeStatus status = NodeStatus.FOLLOWING;
    private volatile int leaderId;
    private volatile long epoch;
    private volatile boolean running = false;

    private RemotingServer nodeServer;
    private RemotingServer nodeMgrServer;
    private RemotingClient client;

    private Vote myVote;

    public Node(final NodeConfig nodeConfig) {
        this.nodeConfig = nodeConfig;
        this.dataManager = DataManager.getInstance();
        this.executorService = new ThreadPoolExecutor(nodeConfig.getCup(), nodeConfig.getMaxPoolSize(),
                nodeConfig.getKeepTime(), TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(nodeConfig.getQueueSize()));
        this.scheduledExecutorService = Executors.newScheduledThreadPool(3);
    }

    public void start() {
        if (running) {
            return;
        }
        synchronized (this) {
            if (running) {
                return;
            }
            // 初始化集群间通信的消息服务器
            nodeMgrServer = new NettyRemotingServer(new NettyServerConfig(nodeConfig.getHost(), nodeConfig.getNodeMgrPort()));
            nodeMgrServer.registerProcessor(MessageType.JOIN_GROUP, new JoinGroupProcessor(this), executorService);
            nodeMgrServer.start();
            // 初始化节点对外提供服务的消息监听器，初始化投票消息，数据同步消息，客户端消息的处理器
            nodeServer = new NettyRemotingServer(new NettyServerConfig(nodeConfig.getHost(), nodeConfig.getPort()));
            nodeServer.registerProcessor(MessageType.VOTE, new VoteRequestProcessor(this), executorService);
            nodeServer.registerProcessor(MessageType.DATA_SYNC, new DataRequestProcessor(this), executorService);
            nodeServer.registerProcessor(MessageType.CLIENT, new ClientRequestProcessor(this), executorService);
            nodeServer.start();
            // 启动监听服务器，监听来自其他节点的消息
            client = new NettyRemotingClient(new NettyClientConfig());
            client.start();
            // 初始化，宣告加入集群的消息，延迟2s后仅执行一次(2s时间用处确保本节点已经初始化)
            scheduledExecutorService.schedule(this::init, 2000, TimeUnit.MILLISECONDS);
            // 选举线程，延迟4s开始选举，时钟500ms(完成一次选举后延迟500ms进行下一次Leader选举)
            scheduledExecutorService.scheduleAtFixedRate(this::election, 4000, 500, TimeUnit.MILLISECONDS);
            // 一次心跳完成后，延迟5s进行下一次心跳
            scheduledExecutorService.scheduleWithFixedDelay(this::heartbeat, 0, nodeConfig.getHeartbeatTimeout(), TimeUnit.MILLISECONDS);
        }
        // 关机钩子
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }

    public void shutdown() {
        synchronized (this) {
            if (nodeMgrServer != null) {
                nodeMgrServer.shutdown();
            }
            if (nodeServer != null) {
                nodeServer.shutdown();
            }
            if (client != null) {
                client.shutdown();
            }
            if (dataManager != null) {
                dataManager.close();
            }
            if (scheduledExecutorService != null) {
                scheduledExecutorService.shutdown();
            }
            if (executorService != null) {
                executorService.shutdown();
            }
            running = false;
        }
    }

    private void init() {
        //初始化自己的节点信息，以便告知其他节点
        JoinGroupMessage joinGroupMsg = JoinGroupMessage.getInstance();
        joinGroupMsg.setNodeId(nodeConfig.getNodeId());
        joinGroupMsg.setHost(nodeConfig.getHost());
        joinGroupMsg.setPort(nodeConfig.getPort());
        joinGroupMsg.setNodeMgrPort(nodeConfig.getNodeMgrPort());

        // 从配置文件中读取节点信息
        for (Map.Entry<Integer, String> entry : nodeConfig.getNodeMgrMap().entrySet()) {
            //跳过本节点，只给集群中其他节点发送消息
            if (entry.getKey() == nodeConfig.getNodeId()) {
                continue;
            }
            executorService.submit(() -> {
                try {
                    RemotingMessage response = client.invokeSync(entry.getValue(), joinGroupMsg.request(), 3 * 1000);
                    //解析消息，json的反序列化
                    JoinGroupMessage res = JoinGroupMessage.getInstance().parseMessage(response);
                    if (res.getSuccess()) {
                        int peerNodeId = res.getNodeId();
                        String host = res.getHost();
                        int port = res.getPort();
                        int nodeMgrPort = res.getNodeMgrPort();
                        // 拿到其他节点的信息，通信端口和客户端端口
                        nodeConfig.getNodeMap().putIfAbsent(peerNodeId, host + ":" + port);
                        nodeConfig.getNodeMgrMap().putIfAbsent(peerNodeId, host + ":" + nodeMgrPort);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
    }

    private void election() {
        // 如果本节点已经是处于LEADING状态，那么不进行选举
        if (status == NodeStatus.LEADING) {
            return;
        }
        // 判断自己的选举计时器是否已经结束，若选举定时器那么就开始本轮投票
        if (!nodeConfig.resetElectionTick()) {
            return;
        }
        // 开始选举的关键，首先修改自己的状态为LOOKING
        status = NodeStatus.LOOKING;
        // 递增epoch，关键，区分不同epoch
        synchronized (this) {
            epoch += 1;
        }
        //清空原有消息缓存 关键！！确保已经丢弃的消息不再出现
        zxIdMap.clear();
        // 投票给自己
        this.myVote = new Vote(nodeConfig.getNodeId(), nodeConfig.getNodeId(), 0, getLastZxId());
        this.myVote.setEpoch(epoch);
        //本地投票箱
        this.voteBox.put(nodeConfig.getNodeId(), myVote);
        // 投票消息
        VoteMessage voteMessage = VoteMessage.getInstance();
        voteMessage.setVote(myVote);

        sendOneWayMsg(voteMessage.request());
    }

    private void heartbeat() {
        // 节点间通信必须有Leader发起
        if (status != NodeStatus.LEADING) {
            return;
        }
        // 判断此次心跳消息与上次心跳消息的间隔，若小于5s则不进行此次心跳
        if (!nodeConfig.resetHeartbeatTick()) {
            return;
        }
        for (Map.Entry<Integer, String> entry : nodeConfig.getNodeMap().entrySet()) {
            // 只会给follow发送心跳消息，从投票箱中判断该节点是否为自己的Follower，只有给自己投过票的节点才有可能是Follower
            if (!voteBox.containsKey(entry.getKey())) {
                continue;
            }
            // 跳过自己
            if (entry.getKey() == nodeConfig.getNodeId()) {
                continue;
            }
            long index = -1;
            //初始化消息id
            if (zxIdMap.containsKey(entry.getKey())) {
                // 优先从缓存中存
                index = zxIdMap.get(entry.getKey()).getCounter();
            } else {
                // 从日志中获取
                index = dataManager.getLastIndex();
            }
            // 更新epoch
            Data data = dataManager.read(index);
            if (data.getZxId().getEpoch() == 0) {
                data.getZxId().setEpoch(epoch);
            }
            // 初始化消息内容
            DataMessage dataMsg = DataMessage.getInstance();
            dataMsg.setNodeId(nodeConfig.getNodeId());
            dataMsg.setType(DataMessage.Type.SYNC);
            dataMsg.setData(data);
            // 发送消息处理消息
            executorService.submit(() -> {
                try {
                    RemotingMessage response = client.invokeSync(entry.getValue(), dataMsg.request(), 3 * 1000);
                    DataMessage res = DataMessage.getInstance().parseMessage(response);
                    if (res.getSuccess()) {
                        int peerId = res.getNodeId();
                        ZxId peerZxId = res.getData().getZxId();
                        zxIdMap.put(peerId, peerZxId);
                    }
                } catch (Exception e) {
                    logger.error(e);
                }
            });
        }
    }

    public void sendOneWayMsg(RemotingMessage msg) {
        for (Map.Entry<Integer, String> entry : nodeConfig.getNodeMap().entrySet()) {
            // 跳过自己
            if (entry.getKey() == nodeConfig.getNodeId()) {
                continue;
            }
            executorService.submit(() -> {
                try {
                    client.invokeOneway(entry.getValue(), msg, 3 * 1000);
                } catch (Exception e) {
                    logger.error(e);
                }
            });
        }
    }

    public boolean isHalf() {
        // 此情况 主要是，不是所有节点都发起了投票
        if (voteBox.size() != nodeConfig.getNodeMap().size()) {
            return false;
        }

        int voteCounter = 0;
        for (Map.Entry<Integer, Vote> entry : voteBox.entrySet()) {
            if (entry.getValue().getVoteId() == myVote.getNodeId()) {
                voteCounter += 1;
            }
        }

        return voteCounter > nodeConfig.getNodeMap().size() / 2;
    }

    public void becomeLeader() {
        this.leaderId = nodeConfig.getNodeId();
        this.status = NodeStatus.LEADING;
    }

    // 从节点受到了写请求，转发给主节点
    public RemotingMessage redirect(RemotingMessage request) {
        RemotingMessage response = null;
        try {
            response = client.invokeSync(nodeConfig.getNodeMap().get(leaderId), request, 3 * 1000);
        } catch (Exception e) {
            logger.error(e);
        }
        return response;
    }

    // 保存数据
    public void appendData(final String key, final String value) {
        Data data = new Data();
        // 此时消息处于发起状态
        data.setKv(new Pair<>(key, value));

        DataMessage dataMessage = DataMessage.getInstance();
        dataMessage.setNodeId(nodeConfig.getNodeId());
        dataMessage.setData(data);
        dataMessage.setType(DataMessage.Type.SNAPSHOT);

        // Leader向其他节点同步数据
        for (Map.Entry<Integer, String> entry : nodeConfig.getNodeMap().entrySet()) {
            if (entry.getKey() == nodeConfig.getNodeId()) {
                continue;
            }

            // 线程池，避免每同步给一个follower都会阻塞等待ACK确认
            // 导致服务器吞吐量降低
            executorService.submit(() -> {
                try {
                    RemotingMessage response = client.invokeSync(entry.getValue(), dataMessage.request(), 3 * 1000);
                    DataMessage resDataMsg = DataMessage.getInstance().parseMessage(response);
                    int peerId = resDataMsg.getNodeId();
                    boolean success = resDataMsg.getSuccess();
                    snapshotMap.put(peerId, success);
                    // 判断数据是否被过半几点记录
                    int snapshotCounter = 0;
                    for (Boolean flag : snapshotMap.values()) {
                        if (flag) {
                            snapshotCounter += 1;
                        }
                    }
                    if (snapshotCounter > nodeConfig.getNodeMap().size() / 2) {
                        // 通过countDownLatch 来标志是否可以提交数据
                        countDownLatch.countDown();
                    }
                } catch (Exception e) {
                    logger.error(e);
                }
            });
        }
    }

    //提交数据
    public boolean commitData(final String key) throws InterruptedException {
        if (countDownLatch.await(6000, TimeUnit.MILLISECONDS)) {
            // 清除本地缓存，避免已被丢弃的消息再次出现
            snapshotMap.clear();
            long lastIndex = dataManager.getLastIndex();
            String value = dataManager.get(key);
            // 消息计数器自增
            ZxId zxId = new ZxId(epoch, lastIndex + 1);
            Pair<String, String> kv = new Pair<>(key, value);
            Data data = new Data(zxId, kv);

            boolean flag = dataManager.write(data);
            if (flag) {
                DataMessage dataMessage = DataMessage.getInstance();
                dataMessage.setNodeId(nodeConfig.getNodeId());
                dataMessage.setData(data);
                dataMessage.setType(DataMessage.Type.COMMIT);

                // 向Follower发送commit指令
                for (Map.Entry<Integer, String> entry : nodeConfig.getNodeMap().entrySet()) {
                    if (entry.getKey() == nodeConfig.getNodeId()) {
                        continue;
                    }
                    executorService.submit(() -> {
                        try {
                            // 无须阻塞等待follower确认，因为commit之前就已经有多数派经行了确认
                            client.invokeOneway(entry.getValue(), dataMessage.request(), 3000);
                        } catch (Exception e) {
                            logger.error(e);
                        }
                    });
                }
            }
            return flag;
        } else {
            return false;
        }
    }

    private ZxId getLastZxId() {
        long lastIndex = dataManager.getLastIndex();
        if (lastIndex == -1) {
            return new ZxId(0, 0);
        } else {
            Data data = dataManager.read(lastIndex);
            return data.getZxId();
        }
    }

    public ConcurrentMap<Integer, Vote> getVoteBox() {
        return voteBox;
    }

    public NodeConfig getNodeConfig() {
        return nodeConfig;
    }

    public DataManager getDataManager() {
        return dataManager;
    }

    public Vote getMyVote() {
        return myVote;
    }

    public NodeStatus getStatus() {
        return status;
    }

    public void setStatus(NodeStatus status) {
        this.status = status;
    }

    public Lock getVoteLock() {
        return voteLock;
    }

    public Lock getDataLock() {
        return dataLock;
    }

    public ConcurrentMap<Integer, ZxId> getZxIdMap() {
        return zxIdMap;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(int leaderId) {
        this.leaderId = leaderId;
    }

    public long getEpoch() {
        return epoch;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    public ConcurrentMap<Integer, Boolean> getSnapshotMap() {
        return snapshotMap;
    }

    public CountDownLatch getCountDownLatch() {
        return countDownLatch;
    }
}
