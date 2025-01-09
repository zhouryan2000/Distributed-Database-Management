package ecs;

import app_kvServer.IKVServer;
import org.apache.log4j.Logger;
import shared.CommunicationModule;
import shared.HashRange;
import shared.Metadata;
import shared.messages.KVMessage;
import shared.messages.KVMessageEntity;

import java.io.IOException;
import app_kvServer.IKVServer.ServerStatus;

public class ECSNode implements IECSNode {
    private static final Logger logger = Logger.getLogger(ECSNode.class);
    private final String name;
    private final String host;
    private final int port;
    private HashRange hashRange;
    private final CommunicationModule ecsCommunicationModule;

    private boolean isFailed;

    public ECSNode(String host, int port, HashRange range, CommunicationModule module) {
        this.host = host;
        this.port = port;
        this.name = host + ":" + String.valueOf(port);
        this.hashRange = range;
        this.ecsCommunicationModule = module;
        this.isFailed = false;
    }

    public void updateFailure(boolean failure_flag) {
        this.isFailed = failure_flag;
    }

    @Override
    public String getNodeName() {
        return this.name;
    }

    @Override
    public String getNodeHost() {
        return this.host;
    }

    @Override
    public int getNodePort() {
        return this.port;
    }

    @Override
    public String[] getNodeHashRange() {
        String[] range = new String[2];
        range[0] = hashRange.getStartIndex().toString(16);
        range[1] = hashRange.getEndIndex().toString(16);
        return range;
    }

    public boolean isNewMessageComing() {
        try {
//            System.out.println(ecsCommunicationModule.getInput().available());
            if (ecsCommunicationModule.getInput().available() <= 2) {
                return false;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    public HashRange getHashRange() {
        return hashRange;
    }

    public boolean updateMetadata(Metadata metadata) throws IOException {
        // update ECS node
        HashRange range = metadata.getHashRange(this.getNodeName());
        if (range == null) {
            logger.fatal("ECS Update hash range failed: cannot get corresponding hash range from metadata");
        }
        this.hashRange = range;

        // ask server in this node to update metadata
        ecsCommunicationModule.
                sendKVMessage(KVMessage.StatusType.UPDATE_SERVER_METADATA, "null", metadata.toString());

        KVMessage response = ecsCommunicationModule.receiveKVMessage();

        if (response.getStatus() != KVMessage.StatusType.UPDATE_SERVER_METADATA_SUCCESS) {
            logger.error(String.format("[Server: %s] did not update its metadata", this.getNodeName()));
            return false;
        }
        else {
            logger.info(String.format("[Server: %s] successfully update its metadata", this.getNodeName()));
            return true;
        }
    }

    public void updateServerStatus(ServerStatus status) throws IOException {
        ecsCommunicationModule.sendKVMessage(KVMessage.StatusType.UPDATE_SERVER_STATUS, "null", status.toString());

        while(true) {
            KVMessage response = ecsCommunicationModule.receiveKVMessage();
            if (response.getStatus() == KVMessage.StatusType.UPDATE_SERVER_STATUS_SUCCESS) {
                break;
            } else {
                logger.error(String.format("[Server: %s] did not update its Server Status", this.getNodeName()));
            }
        }
    }

    public boolean startRebalance(ECSNode dst) throws IOException {
        HashRange dstRange = dst.getHashRange();

        ecsCommunicationModule.sendKVMessage(KVMessage.StatusType.REBALANCE, dst.getNodeName(), dstRange.toString());

        // whether it's success?
        KVMessage response = ecsCommunicationModule.receiveKVMessage();

        if (response.getStatus() == KVMessage.StatusType.REBALANCE_SUCCESS) {
            return true;
        } else if (response.getStatus() == KVMessage.StatusType.REBALANCE_ERROR) {
            return false;
        } else {
            logger.fatal("Unexpected rebalance message received from server " + this.name);
            return false;
        }
    }

    public CommunicationModule getEcsCommunicationModule() {
        return ecsCommunicationModule;
    }

    /*M3*/
    public void heartbeating() {
        try {
            ecsCommunicationModule.sendKVMessage(KVMessage.StatusType.IS_ALIVE, null, null);
            KVMessage response = ecsCommunicationModule.receiveKVMessage();

            if (response.getStatus() != KVMessage.StatusType.ALIVE) {
                logger.fatal("Unexpected message received when heartbeating");
                logger.info(response.getStatus());
            }
        } catch(Exception e) {
            this.isFailed = true;
            logger.info("=======================heartbeating===========================");
            logger.info(String.format("Detect Node [%s] failed\n", this.name));
        }
    }

    public boolean isNodeFailed() {
        return this.isFailed;
    }

    public void transferKVPairs(ECSNode dst, HashRange range) throws IOException {
        ecsCommunicationModule.sendKVMessage(KVMessage.StatusType.TRANSFER_DATA, dst.getNodeName(), range.toString());
        KVMessage response = ecsCommunicationModule.receiveKVMessage();

        if(response.getStatus() == KVMessage.StatusType.TRANSFER_DATA_SUCCESS) {
            logger.info(String.format("Transfer from node [%s] to [%s] success", this.name, dst.getNodeName()));
        }
        else {
            logger.error(String.format("Transfer from node [%s] to [%s] error", this.name, dst.getNodeName()));
        }
    }

    public void deleteKVPairs(HashRange range) throws IOException {
        ecsCommunicationModule.sendKVMessage(KVMessage.StatusType.DELETE_DATA, range.toString(), null);
        KVMessage response = ecsCommunicationModule.receiveKVMessage();

        if(response.getStatus() == KVMessage.StatusType.DELETE_DATA_SUCCESS) {
            logger.info(String.format("Delete data from node [%s] success", this.name));
        }
        else {
            logger.error(String.format("Delete data from node [%s] failed", this.name));
        }
    }

}
