package shared;

import app_kvServer.KVServer;
import org.apache.log4j.Logger;
import shared.messages.KVMessage;
import shared.messages.KVMessageEntity;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import static shared.messages.KVMessage.StatusType;

public class ECSCommunicationModule extends CommunicationModule {
    private static Logger logger = Logger.getLogger(ECSCommunicationModule.class);

    private Socket clientSocket;
    private KVServer kvServer; // if kvServer is null, this is client side; otherwise, server side

    public ECSCommunicationModule(Socket clientSocket, KVServer kvServer) {
        super(clientSocket, kvServer);
        this.kvServer = kvServer;
        this.clientSocket = clientSocket;
    }

    @Override
    protected KVMessage handleReceivedKVMessage(KVMessage message) throws Exception {
//        logger.info("In child class");

        KVMessage result = new KVMessageEntity(KVMessage.StatusType.FAILED, "unknown error", null);;

        try{
            if (message != null){
                String key = message.getKey();
                String value = message.getValue();
                KVMessage.StatusType status = message.getStatus();
                KVMessage.StatusType resultStatus;
                switch (status){
                    case TRANSFER_DATA:
                        if (kvServer.transferKVPairs(key, value)) { // dstNode, range
                            result.setStatus(StatusType.TRANSFER_DATA_SUCCESS);
                            result.setKey("success");
                        }
                        else {
                            result.setStatus(StatusType.TRANSFER_DATA_ERROR);
                        }
                        break;
                    case DELETE_DATA:
                        kvServer.deleteKVPairs(key);
                        result.setStatus(StatusType.DELETE_DATA_SUCCESS);
                        result.setKey("success");
                        break;
                    case UPDATE_SERVER_METADATA:
                        if (kvServer.updateMetadata(value)) {
                            result.setStatus(StatusType.UPDATE_SERVER_METADATA_SUCCESS);
                            result.setKey("success");
                        }
                        break;
                    case UPDATE_SERVER_STATUS:
                        kvServer.setServerStatus(value);
                        result.setStatus(StatusType.UPDATE_SERVER_STATUS_SUCCESS);
                        result.setKey("success");
                        break;
                    case REBALANCE:
                        if (kvServer.rebalance(key, value)) {
                            result.setStatus(StatusType.REBALANCE_SUCCESS);
                            result.setKey("success");
                        }
                        else {
                            result.setStatus(StatusType.REBALANCE_ERROR);
                        }
                        break;
                    case IS_ALIVE:
                        result = new KVMessageEntity(StatusType.ALIVE, null, null);
                        break;
                    case UNKNOWN:
                        result = new KVMessageEntity(KVMessage.StatusType.FAILED, "message format unknown", null);
                        break;
                    default:
                        break;
                }
            }
        } catch (Exception e) {
            logger.error("Unexpected Error during handle ECS request", e);
        }

        return result;
    }
}
