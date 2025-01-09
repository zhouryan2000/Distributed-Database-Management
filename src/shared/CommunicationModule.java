package shared;

import app_kvServer.IKVServer.ServerStatus;
import app_kvServer.KVServer;
import org.apache.log4j.Logger;
import org.json.*;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;
import shared.messages.KVMessageEntity;
import java.io.*;
import java.net.Socket;

public class CommunicationModule implements Runnable {
    private static Logger logger = Logger.getLogger(CommunicationModule.class);

    private boolean isOpen;
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;

    private Socket clientSocket;
    private InputStream input;
    private OutputStream output;
    private KVServer kvServer; // if kvServer is null, this is client side; otherwise, server side

    public CommunicationModule(Socket clientSocket, KVServer kvServer) {
        this.clientSocket = clientSocket;
        this.kvServer = kvServer;
        this.isOpen = true;
        try {
            this.output = clientSocket.getOutputStream();
            this.input = clientSocket.getInputStream();
        }
        catch (IOException e) {
            logger.error("Error!", e);
        }
    }

    public InputStream getInput() {
        return input;
    }

    public boolean isClosed() {
        return this.clientSocket.isClosed();
    }

    /**
     * Initializes and starts the client connection.
     * Loops until the connection is closed or aborted by the client.
     */
    public void run() {
        try {
            output = clientSocket.getOutputStream();
            input = clientSocket.getInputStream();

            if (kvServer != null) { // server-side communication
                while (isOpen) {
                    try {
                        KVMessage message = receiveKVMessage();
                        KVMessage reply = handleReceivedKVMessage(message);
                        sendKVMessage(reply.getStatus(), reply.getKey(), reply.getValue());
                    } catch (IOException ioe) {
                        if (isOpen) {
                            logger.warn("Error! Connection lost!");
                            isOpen = false;
                        }
                    } catch (Exception e) {
                        logger.error(e);
                    }
                }

                // Do it again if there is new message come in
                try {
                    KVMessage message = receiveKVMessage();
                    KVMessage reply = handleReceivedKVMessage(message);
                    sendKVMessage(reply.getStatus(), reply.getKey(), reply.getValue());
                } catch (IOException ioe) {
                    if (isOpen) {
                        logger.warn("Error! Connection lost!");
                        isOpen = false;
                    }
                } catch (Exception e) {
                    logger.error(e);
                }
            }
        } catch (IOException ioe) {
            logger.error("Error! Connection could not be established!", ioe);

        } finally {
            closeCommunicationModule();
        }
    }

    /**
     * Method sends a TextMessage using this socket.
     * @param status
     * @param key
     * @param value
     * @throws IOException some I/O error regarding the output stream
     */
    public KVMessage sendKVMessage(StatusType status, String key, String value) throws IOException {
        KVMessage message= new KVMessageEntity(status, key, value);
//        logger.debug("KVMessage Sent:  " + message.outputFormat() + "\n");

        byte[] msgBytes = message.toByteArray();
        output.write(msgBytes, 0, msgBytes.length);
        output.flush();

        return message;
    }

    public KVMessage receiveKVMessage() throws IOException {
        int index = 0;
        byte[] msgBytes = null, tmp = null;
        byte[] bufferBytes = new byte[BUFFER_SIZE];

        /* read first char from stream */
        byte read = (byte) input.read();
        boolean reading = true;

        while(read != 10 && read != -1 && reading) {/* '\n', disconnect, error */
            /* if buffer filled, copy to msg array */
            if(index == BUFFER_SIZE) {
                if(msgBytes == null){
                    tmp = new byte[BUFFER_SIZE];
                    System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
                } else {
                    tmp = new byte[msgBytes.length + BUFFER_SIZE];
                    System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
                    System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
                            BUFFER_SIZE);
                }

                msgBytes = tmp;
                bufferBytes = new byte[BUFFER_SIZE];
                index = 0;
            }

            /* only read valid characters, i.e. letters and constants */
            bufferBytes[index] = read;
            index++;

            /* stop reading is DROP_SIZE is reached */
            if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
                reading = false;
            }

            /* read next char from stream */
            read = (byte) input.read();
        }

        if(msgBytes == null){
            tmp = new byte[index];
            System.arraycopy(bufferBytes, 0, tmp, 0, index);
        } else {
            tmp = new byte[msgBytes.length + index];
            System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
            System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
        }

        msgBytes = tmp;

        if(msgBytes.length < 2) {
            throw new IOException("Error! Connection lost!");
        }

        KVMessage message = new KVMessageEntity(msgBytes);
//        logger.debug("Receive message: " + message.outputFormat() + "\n");

        return message;
    }

    protected KVMessage handleReceivedKVMessage(KVMessage message) throws Exception {
        KVMessage result = new KVMessageEntity(StatusType.FAILED, "unknown error", null);;

        try{
            if (message != null){
                String key = message.getKey();
                String value = message.getValue();
                StatusType status = message.getStatus();
                StatusType resultStatus;

                switch (status){
                    case SERVER_PUT:
                        if (kvServer.inStorage(key)) {
                            resultStatus = StatusType.PUT_UPDATE;
                        }
                        else {
                            resultStatus = StatusType.PUT_SUCCESS;
                        }
                        kvServer.putKV(key, value);
                        result = new KVMessageEntity(resultStatus, key, value);
                        break;
                    case REPLICA_PUT:
                        if (kvServer.inStorage(key)) {
                            resultStatus = StatusType.PUT_UPDATE;
                        }
                        else {
                            resultStatus = StatusType.PUT_SUCCESS;
                        }
                        kvServer.putKVReplica(key, value);
                        result = new KVMessageEntity(resultStatus, key, value);
                        break;
                    case REPLICA_DELETE:
                        if (kvServer.getServerStatus()== ServerStatus.WRITE_LOCK) {
                            resultStatus = StatusType.SERVER_WRITE_LOCK;
                            key = null;
                            value = null;
                        } else if (kvServer.getServerStatus() == ServerStatus.STOP) {
                            resultStatus = StatusType.SERVER_STOPPED;
                            key = null;
                            value = null;
                        } else {
                            if (kvServer.inStorage(key)) {
                                kvServer.putKVReplica(key, value);
                                resultStatus = StatusType.DELETE_SUCCESS;
                            } else {
                                resultStatus = StatusType.DELETE_ERROR;
                            }
                        }
                        result = new KVMessageEntity(resultStatus, key, value);
                        break;
                    case PUT:
                        if (key.isEmpty() || key.length() > 20 || key.contains(" ") || (value != null && value.length() > 122880)) {
                            result = new KVMessageEntity(StatusType.FAILED, "message size exceeded", null);
                        } else {
//                            HashRange range = kvServer.serverMetadata.getHashRange(kvServer.name);
//                            if (range.startIndex > key || range.endIndex < key) {
//                                resultStatus = StatusType.SERVER_NOT_RESPONSIBLE;
                            if (!kvServer.isInCoordinatorRange(key)) {
                                resultStatus = StatusType.SERVER_NOT_RESPONSIBLE;
                                key = null;
                                value = null;
                            } else if (kvServer.getServerStatus()== ServerStatus.WRITE_LOCK) {
                                resultStatus = StatusType.SERVER_WRITE_LOCK;
                                key = null;
                                value = null;
                            } else if (kvServer.getServerStatus() == ServerStatus.STOP) {
                                resultStatus = StatusType.SERVER_STOPPED;
                                key = null;
                                value = null;
                            } else {
                                if (value != null && !value.equals("null"))  {
                                    if (kvServer.inStorage(key)) {
                                        resultStatus = StatusType.PUT_UPDATE;
                                    }
                                    else {
                                        resultStatus = StatusType.PUT_SUCCESS;
                                    }
                                    kvServer.putKV(key, value);
                                } else {
                                    if (kvServer.inStorage(key)) {
                                        kvServer.putKV(key, value);
                                        resultStatus = StatusType.DELETE_SUCCESS;
                                    } else {
                                        resultStatus = StatusType.DELETE_ERROR;
                                    }
                                }
                            }
                            result = new KVMessageEntity(resultStatus, key, value);
                        }
                        break;
                    case GET:
                        if (key.isEmpty() || key.length() > 20) {
                            result = new KVMessageEntity(StatusType.GET_ERROR, key, null);
                        } else {
                            String val = "null";
//                            HashRange range = kvServer.serverMetadata.getHashRange(kvServer.name);
//                            if (range.startIndex > key || range.endIndex < key) {
//                                resultStatus = StatusType.SERVER_NOT_RESPONSIBLE;
                            if (!kvServer.isInReplicaRange(key)) {
                                resultStatus = StatusType.SERVER_NOT_RESPONSIBLE;
                                key = null;
                                val = null;
                            } else if (kvServer.getServerStatus() == ServerStatus.STOP) {
                                resultStatus = StatusType.SERVER_STOPPED;
                                key = null;
                                val = null;
                            } else {
                                val = kvServer.getKV(key);
                                if (val != null) {
                                    resultStatus = StatusType.GET_SUCCESS;
                                }
                                else {
                                    resultStatus = StatusType.GET_ERROR;
                                }
                            }
                            result = new KVMessageEntity(resultStatus, key, val);
                        }
                        break;
                    case KEYRANGE:
                        if (kvServer.getServerStatus() == ServerStatus.STOP) {
                            resultStatus = StatusType.SERVER_STOPPED;
                            key = null;
                        } else {
                            resultStatus = StatusType.KEYRANGE_SUCCESS;
                            key = kvServer.getServerMetadata();
                        }
                        result = new KVMessageEntity(resultStatus, key, null);
                        break;
                    case KEYRANGE_READ:
                        if (kvServer.getServerStatus() == ServerStatus.STOP) {
                            resultStatus = StatusType.SERVER_STOPPED;
                            key = null;
                        } else {
                            resultStatus = StatusType.KEYRANGE_READ_SUCCESS;
                            key = kvServer.getServerMetadataWithReplica();
                        }
                        result = new KVMessageEntity(resultStatus, key, null);
                        break;
                    case SELECT_QUERY:
                        String val = "null";
                        if (!kvServer.isInReplicaRange(key)) {
                            resultStatus = StatusType.SERVER_NOT_RESPONSIBLE;
                            key = null;
                            val = null;
                        } else if (kvServer.getServerStatus() == ServerStatus.STOP) {
                            resultStatus = StatusType.SERVER_STOPPED;
                            key = null;
                            val = null;
                        } else {
                            val = kvServer.selectQuery(key, value);
                            if (val != null) {
                                resultStatus = StatusType.QUERY_SUCCESS;
                                key = "success";
                            }
                            else {
                                resultStatus = StatusType.GET_ERROR;
                            }
                        }
                        result = new KVMessageEntity(resultStatus, key, val);
                        break;
                    case UPDATE_QUERY:
                        if (!kvServer.isInCoordinatorRange(key)) {
                            resultStatus = StatusType.SERVER_NOT_RESPONSIBLE;
                            key = null;
                            value = null;
                        } else if (kvServer.getServerStatus()== ServerStatus.WRITE_LOCK) {
                            resultStatus = StatusType.SERVER_WRITE_LOCK;
                            key = null;
                            value = null;
                        } else if (kvServer.getServerStatus() == ServerStatus.STOP) {
                            resultStatus = StatusType.SERVER_STOPPED;
                            key = null;
                            value = null;
                        } else {
                            if (kvServer.updateQuery(key, value)) {
                                resultStatus = StatusType.QUERY_SUCCESS;
                            }
                            else {
                                resultStatus = StatusType.QUERY_ERROR;
                            }
                        }
                        result = new KVMessageEntity(resultStatus, key, value);
                        break;
                    case DELETE_QUERY:
                        if (!kvServer.isInCoordinatorRange(key)) {
                            resultStatus = StatusType.SERVER_NOT_RESPONSIBLE;
                            key = null;
                            value = null;
                        } else if (kvServer.getServerStatus()== ServerStatus.WRITE_LOCK) {
                            resultStatus = StatusType.SERVER_WRITE_LOCK;
                            key = null;
                            value = null;
                        } else if (kvServer.getServerStatus() == ServerStatus.STOP) {
                            resultStatus = StatusType.SERVER_STOPPED;
                            key = null;
                            value = null;
                        } else {
                            if (kvServer.deleteQuery(key, value)) {
                                resultStatus = StatusType.QUERY_SUCCESS;
                            }
                            else {
                                resultStatus = StatusType.QUERY_ERROR;
                            }
                        }
                        result = new KVMessageEntity(resultStatus, key, value);
                        break;
                    case UNKNOWN:
                        result = new KVMessageEntity(StatusType.FAILED, "message format unknown", null);
                    default:
                        break;
                }
            }
        } catch (Exception e) {
            logger.error("Unexpected Error during handle client request", e);
        }

        return result;
    }

    public void closeCommunicationModule(){
        try {
            if (clientSocket != null) {
                this.isOpen = false;
                input.close();
                output.close();
                clientSocket.close();
            }
        } catch (IOException ioe) {
            logger.error("Error! Unable to tear down connection!", ioe);
        }
    }

    public void setClose() {
        this.isOpen = false;
    }
}