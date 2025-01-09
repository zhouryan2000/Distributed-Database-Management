package app_kvECS;

import java.io.IOException;
import java.math.BigInteger;
import java.net.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Collection;

import app_kvServer.IKVServer;
import ecs.ECSNode;
import ecs.IECSNode;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.CommunicationModule;
import shared.ECSCommunicationModule;
import shared.HashRange;
import shared.Metadata;
import shared.messages.KVMessage;
import shared.Pair;

public class ECSClient implements IECSClient {
    private static final Logger logger = Logger.getLogger(ECSClient.class);
    private final String address;
    private final int port;
    private Metadata metadata;
    private Map<String, ECSNode> ecsNodes;
    private ServerSocket ecsSocket;
    private boolean running;

    public ECSClient(String address, int port) {
        this.address = address;
        this.port = port;
        this.metadata = new Metadata();
        this.ecsNodes = new HashMap<>();
    }

    public Map getECSNode() {
        return this.ecsNodes;
    }

    public boolean initializeECS() {
        logger.info("Initializing ECS ...");
        try {
            InetAddress inetAddress = InetAddress.getByName(this.address);
            ecsSocket = new ServerSocket(port, 50, inetAddress);
            ecsSocket.setSoTimeout(200);
            logger.info("ECS listening on " + address + ":" + port);
            return true;

        } catch (IOException e) {
            logger.error("Error! Cannot open server socket:");
            if(e instanceof BindException){
                logger.error("Port " + port + " is already bound!");
            }
            return false;
        }
    }

//    private void createECSNode(String hostname, int port, CommunicationModule communicationModule) throws IOException {
//        Pair<HashRange, String> result = this.metadata.addServer(hostname, port);
//        HashRange hashRange = result.fst;
//        String successorName = result.snd;
//
//        ECSNode newNode = new ECSNode(hostname, port, hashRange, communicationModule);
//
//        if (successorName == null) { // first server connected to ECS
//            if (!ecsNodes.isEmpty()) {
//                logger.fatal("ECSNodes list should be empty!");
//            }
//            if(newNode.updateMetadata(metadata)) {
//                newNode.updateServerStatus(IKVServer.ServerStatus.ACTIVE);
//            }
//        }
//        else {
//            ECSNode succesorNode = ecsNodes.get(successorName);
//            if (rebalance(newNode, succesorNode)) {
//
//                // after rebalance, set it to active
//                newNode.updateServerStatus(IKVServer.ServerStatus.ACTIVE);
//                succesorNode.updateServerStatus(IKVServer.ServerStatus.ACTIVE);
//            }
//        }
//
//        ecsNodes.put(newNode.getNodeName(), newNode);
//        logger.info(String.format("Successfully add ECSNode [%s] to the cluster", newNode.getNodeName()));
//    }

    private void createECSNodeWithReplica(String hostname, int port, CommunicationModule communicationModule) throws IOException {
        Pair<HashRange, String> result = this.metadata.addServer(hostname, port);
        HashRange hashRange = result.fst;
        String successorName = result.snd;

        ECSNode newNode = new ECSNode(hostname, port, hashRange, communicationModule);

        if (successorName == null) { // first server connected to ECS
            if (!ecsNodes.isEmpty()) {
                logger.fatal("ECSNodes list should be empty!");
            }
            if(newNode.updateMetadata(metadata)) {
                newNode.updateServerStatus(IKVServer.ServerStatus.ACTIVE);
            }

            ecsNodes.put(newNode.getNodeName(), newNode);
            logger.info(String.format("Successfully add ECSNode [%s] to the cluster", newNode.getNodeName()));

            return;
        }

        ECSNode successorNode = ecsNodes.get(successorName);
//        if (rebalance(newNode, successorNode)) {
//
//            // after rebalance, set it to active
//            successorNode.updateServerStatus(IKVServer.ServerStatus.ACTIVE);
//        }

        ecsNodes.put(newNode.getNodeName(), newNode);
        logger.info(String.format("Successfully add ECSNode [%s] to the cluster", newNode.getNodeName()));
        newNode.updateMetadata(metadata);

        if (metadata.getSize() == 2 || metadata.getSize() == 3) {
            // transfer all data to new node
            successorNode.transferKVPairs(newNode,
                    new HashRange(
                            new BigInteger("0", 16),
                            new BigInteger("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", 16)));
        }

        if (this.metadata.getSize() > 3) {
            Pair<String, HashRange> sndPredecessor = metadata.getSuccessorAt(newNode.getNodeName(), -2);
            Pair<String, HashRange> fstPredecessor = metadata.getSuccessorAt(newNode.getNodeName(), -1);
            ECSNode sndSuccessor = ecsNodes.get(metadata.getSuccessorAt(newNode.getNodeName(), 2).getFirst());
            ECSNode trdSuccessor = ecsNodes.get(metadata.getSuccessorAt(newNode.getNodeName(), 3).getFirst());

            // store replica of two predecessors
            HashRange range1 = new HashRange(sndPredecessor.getSecond().getStartIndex(),
                    newNode.getHashRange().getEndIndex());

            successorNode.transferKVPairs(newNode, range1);

            // successorNode no longer store replica of sndPredecessor
            successorNode.deleteKVPairs(sndPredecessor.getSecond());

            // similarly
            sndSuccessor.deleteKVPairs(fstPredecessor.getSecond());

            trdSuccessor.deleteKVPairs(newNode.getHashRange());
        }

        for (ECSNode node : ecsNodes.values()) {
            node.updateMetadata(metadata);
        }

        newNode.updateServerStatus(IKVServer.ServerStatus.ACTIVE);
    }


    private boolean rebalance(ECSNode dstNode, ECSNode sourceNode) throws IOException {
        dstNode.updateMetadata(metadata);

        sourceNode.startRebalance(dstNode);

        for (ECSNode node : ecsNodes.values()) {
            node.updateMetadata(metadata);
        }

        return true;
    }

    public synchronized void removeFailedNode(ECSNode removed_node) {
        // The naming convention is based on the position of removed_node in hash ring before remove it

        ecsNodes.remove(removed_node.getNodeName());
        Metadata metadataBefore = new Metadata(metadata.toString());
        Pair<String, HashRange> successor = metadata.removeServer(removed_node.getNodeHost(), removed_node.getNodePort());

        if (successor != null) {
            String successorName = successor.getFirst();
            ECSNode successorNode = ecsNodes.get(successorName);
            try {

//                rebalance(successorNode, removed_node);

                if (metadata.getSize() >= 3) {
                    ECSNode sndSuccessor = ecsNodes.get(metadataBefore.getSuccessorAt(
                            removed_node.getNodeName(), 2).getFirst());
                    ECSNode trdSuccessor = ecsNodes.get(metadataBefore.getSuccessorAt(
                            removed_node.getNodeName(), 3).getFirst());
                    ECSNode fstPredecessor = ecsNodes.get(metadataBefore.getSuccessorAt(
                            removed_node.getNodeName(), -1).getFirst());
                    ECSNode sndPredecessor = ecsNodes.get(metadataBefore.getSuccessorAt(
                            removed_node.getNodeName(), -2).getFirst());

                    // transfer from successor to
                    // new replica data added to 2nd successor of removed_node's successor
                    successorNode.transferKVPairs(trdSuccessor, removed_node.getHashRange());

                    // new replica data added to 1st successor of removed_node's successor
                    fstPredecessor.transferKVPairs(sndSuccessor, fstPredecessor.getHashRange());

                    sndPredecessor.transferKVPairs(successorNode, sndPredecessor.getHashRange());
                }

//                if (!isFailed) {
//                    removed_node.deleteKVPairs(removed_node.getHashRange());
//                    removed_node.updateServerStatus(IKVServer.ServerStatus.STOP);
//                }

                for (ECSNode node : ecsNodes.values()) {
                    node.updateMetadata(metadata);
                }

            } catch (IOException e) {
                logger.error("Rebalance Connection lost when removing node", e);
            }
        }

        logger.info(String.format("Successfully remove node [%s]", removed_node.getNodeName()));
    }

    private void monitoringECSNodes()  {
//        ArrayList<ECSNode> shutdownNodes = new ArrayList<>();
        ArrayList<ECSNode> failedNodes = new ArrayList<>();

//        logger.info(ecsNodes.size());

        for (ECSNode node: ecsNodes.values()) {

            node.heartbeating();

            if (node.isNodeFailed()) {
                failedNodes.add(node);
            }

//            if (node.isNodeShutdown()) {
//                shutdownNodes.add(node);
//            }

//            if (!node.isNewMessageComing()) continue;
//
//            try {
//                KVMessage request = node.getEcsCommunicationModule().receiveKVMessage();
//
//                // only SHUTDOWN message should come at this point
//                if (request.getStatus() == KVMessage.StatusType.SHUTDOWN) {
//                    shutdownNodes.add(node);
//                    logger.debug("ECS received shutdown message from " + node.getNodeName());
//                } else {
//                    logger.error("ECS received unexpected message from " + node.getNodeName());
//                }
//            } catch (IOException e) {
//                logger.error(String.format("Lost connection to server [%s]", node.getNodeName()));
//            }
        }

//        for (ECSNode node: shutdownNodes) {
//            removeNode(node, false);
//        }

        for (ECSNode node: failedNodes) {
            removeFailedNode(node);
        }
    }

    public void run() {
        running = initializeECS();
        if(ecsSocket != null) {
            while(isRunning()){
//                System.out.println("running!");
                try {
                    Socket serverSocket = ecsSocket.accept();
                    CommunicationModule communicationModule = new ECSCommunicationModule(serverSocket, null);

                    logger.info("ECS successfully connected to server "
                            + serverSocket.getInetAddress().getHostName()
                            +  " on port " + serverSocket.getPort());

                    KVMessage message = communicationModule.receiveKVMessage();
                    if (message.getStatus() != KVMessage.StatusType.ECS_CONNECT) {
                        logger.error("Error! Incorrect message from KVServer");
                        communicationModule.closeCommunicationModule();
                        return;
                    }

                    String hostname = message.getKey();
                    int port = Integer.parseInt(message.getValue());

                    logger.info(String.format("The server at [%s:%d] is successfully connrected to ECS at %s:%d",
                            hostname, port, serverSocket.getInetAddress().getHostName(), serverSocket.getLocalPort()));

                    communicationModule.sendKVMessage(KVMessage.StatusType.ECS_CONNECT_SUCCESS, "status", "successful");

                    createECSNodeWithReplica(hostname, port, communicationModule);
                } catch (SocketTimeoutException e){

                } catch (IOException e) {
                    logger.error("Error! " +
                            "Unable to establish connection. \n", e);
                }

                monitoringECSNodes();
            }
        }
        logger.info("ECS Server stopped.");
    }

    private boolean isRunning(){
        return this.running;
    }

    @Override
    public boolean start() {
        // TODO
        return false;
    }

    @Override
    public boolean stop() {
        // TODO
        return false;
    }

    @Override
    public boolean shutdown() {
        // TODO
        return false;
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        // TODO
        return false;
    }

    @Override
    public Map<String, ECSNode> getNodes() {
        // TODO
        return ecsNodes;
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        // TODO
        return ecsNodes.get(Key);
    }

    public static void main(String[] args) {
        int port=-1;
        String address = "localhost";
        Level logLevel = Level.ALL;
        String logdir = "logs/server.log";
        try {
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "-p":
                        port = Integer.parseInt(args[++i]);
                        break;
                    case "-a":
                        address = args[++i];
                        break;
                    case "-l":
                        logdir = args[++i];
                        break;
                    case "-ll":
                        String levelString = args[++i];
                        if(levelString.equals(Level.ALL.toString())) {
                            logLevel = Level.ALL;
                        } else if(levelString.equals(Level.DEBUG.toString())) {
                            logLevel = Level.DEBUG;
                        } else if(levelString.equals(Level.INFO.toString())) {
                            logLevel = Level.INFO;
                        } else if(levelString.equals(Level.WARN.toString())) {
                            logLevel = Level.WARN;
                        } else if(levelString.equals(Level.ERROR.toString())) {
                            logLevel = Level.ERROR;
                        } else if(levelString.equals(Level.FATAL.toString())) {
                            logLevel = Level.FATAL;
                        } else if(levelString.equals(Level.OFF.toString())) {
                            logLevel = Level.OFF;
                        }
                        break;
                    default:
                        // Handle unknown arguments
                        System.out.println("Unknown argument: " + args[i]);
                        break;
                }
            }
            if (port == -1) {
                System.out.println("Error! There is no port!");
                System.exit(1);
            }
            new LogSetup(logdir, logLevel);
            ECSClient ecsClient = new ECSClient(address, port);
            ecsClient.run();
        } catch (IOException e) {
            logger.error("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        } catch (NumberFormatException nfe) {
            System.out.println("Error! Invalid argument <port>! Not a number!");
            System.out.println(port);
            System.out.println("Usage: Server <port>");
            System.exit(1);
        }
    }
}
