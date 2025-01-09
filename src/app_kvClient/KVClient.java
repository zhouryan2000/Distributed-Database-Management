package app_kvClient;

import client.KVCommInterface;
import client.KVStore;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.util.ArrayList;

public class KVClient implements IKVClient {
    private static final Logger logger = Logger.getLogger(KVClient.class);
    private static final String PROMPT = "KVClient> ";
    private BufferedReader stdin;
    private boolean stop = false;
    private KVStore kvStore = null;

    private String serverAddress;
    private int serverPort;

    @Override
    public void newConnection(String hostname, int port) throws Exception{
        kvStore = new KVStore(hostname, port);
        kvStore.connect();
        this.serverAddress = hostname;
        this.serverPort = port;
    }

    @Override
    public KVCommInterface getStore(){
        return kvStore;
    }

    public void run() {
        while(!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String cmdLine = stdin.readLine();
                this.handleCommand(cmdLine);
            } catch (IOException e) {
                stop = true;
                //CLI part error
                System.out.println("ERROR");
            }
        }
    }

    private void handleCommand(String cmdLine) {
        String[] tokens = cmdLine.split("\\s+");

        if(tokens[0].equals("quit")) {
            stop = true;
            if (kvStore != null) {
                kvStore.disconnect();
            }
            kvStore = null;
            System.out.println(PROMPT + "Application exit!");

        } else if (tokens[0].equals("connect")){
            if(tokens.length == 3) {
                try{
                    serverAddress = tokens[1];
                    serverPort = Integer.parseInt(tokens[2]);
                    newConnection(serverAddress, serverPort);
                    System.out.println(PROMPT + "Successfully connect to server");
                } catch(NumberFormatException nfe) {
                    printError("No valid address. Port must be a number!");
                    logger.info("Unable to parse argument <port>", nfe);
                } catch (UnknownHostException e) {
                    printError("Unknown Host!");
                    logger.info("Unknown Host!", e);
                } catch (IOException e) {
                    printError("Could not establish connection!");
                    logger.warn("Could not establish connection!");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                printError("Invalid number of parameters for connect!");
            }

        } else if (tokens[0].equals("put")) {

            if (tokens.length == 3) {
                if (kvStore == null || !kvStore.isRunning()) {
                    System.out.println(PROMPT + "Error! Not connected to server");
                    return;
                }
                if (kvStore != null && kvStore.isRunning()) {
                    String key = tokens[1];
                    if (key.getBytes().length > 20) {
                        logger.error("Key length exceeds 20 Bytes");
                        return;
                    }
                    String value = tokens[2];
                    if (key.getBytes().length > 122880) {
                        logger.error("Value length exceeds 120 kBytes");
                        return;
                    }

                    logger.info("sending put request, key: " + key + ", value: " + value);

                    try {
                        KVMessage message = kvStore.put(key, value);
                        if (message.getStatus() == StatusType.PUT_SUCCESS || message.getStatus() == StatusType.PUT_UPDATE
                                || message.getStatus() == StatusType.DELETE_SUCCESS) {
                            System.out.println(PROMPT + message.outputFormat());
                        }
                        else if (message.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE || message.getStatus() == StatusType.SERVER_WRITE_LOCK
                        || message.getStatus() == StatusType.SERVER_STOPPED) {
                            System.out.println(PROMPT + "Sorry! SERVER STATUS: " + message.getStatus());
                        }
                        else {
                            System.out.println(PROMPT + "ERROR! RECEIVE UNEXPECTED MESSAGE");
                        }
                    } catch (Exception e) {
                        logger.error(e);
                    }
                } else {
                    printError("Not connected");
                }
            } else {
                printError("Invalid number of parameters for put!");
            }

        } else if (tokens[0].equals("get")) {

            if (tokens.length == 2) {
                if (kvStore == null || !kvStore.isRunning()) {
                    System.out.println(PROMPT + "Error! Not connected to server");
                    return;
                }
                if (kvStore != null && kvStore.isRunning()) {
                    String key = tokens[1];
                    if (key.getBytes().length > 20) {
                        logger.error("Key length exceeds 20 Bytes");
                        return;
                    }

                    logger.debug("sending get request, key: " + key);

                    try {
                        KVMessage message = kvStore.get(key);
                        if (message.getStatus() == KVMessage.StatusType.GET_SUCCESS || message.getStatus() == KVMessage.StatusType.GET_ERROR) {
                            System.out.println(PROMPT + message.outputFormat());
                        }
                        else if (message.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE || message.getStatus() == StatusType.SERVER_STOPPED) {
                            System.out.println(PROMPT + "Sorry! SERVER STATUS: " + message.getStatus());
                        }
                        else {
                            System.out.println(PROMPT + "ERROR! RECEIVE UNEXPECTED MESSAGE");
                        }
                    } catch (Exception e) {
                        logger.error(e);
                    }
                } else {
                    printError("Not connected");
                }
            } else {
                printError("Invalid number of parameters for get!");
            }

        } else if(tokens[0].equals("keyrange")) {

            if (kvStore == null || !kvStore.isRunning()) {
                System.out.println(PROMPT + "Error! Not connected to server");
                return;
            }
            if (kvStore != null && kvStore.isRunning()) {
                try {
                    KVMessage message = kvStore.keyrange();
                    if (message.getStatus() == KVMessage.StatusType.KEYRANGE_SUCCESS) {
                        // TODO
                        System.out.println(PROMPT + "Status: " + message.getStatus() +
                                "\n" + PROMPT + "keyrange: " + message.getKey());
                    }
                    else if (message.getStatus() == StatusType.SERVER_STOPPED) {
                        System.out.println(PROMPT + "SORRY! SERVER STATUS: " + message.getStatus());
                    }
                } catch (Exception e) {
                    logger.error(e);
                }
            } else {
                printError("Not connected");
            }

        } else if(tokens[0].equals("keyrange_read")) {

            if (kvStore == null || !kvStore.isRunning()) {
                System.out.println(PROMPT + "Error! Not connected to server");
                return;
            }
            if (kvStore != null && kvStore.isRunning()) {
                try {
                    KVMessage message = kvStore.keyrangeRead();
                    if (message.getStatus() == StatusType.KEYRANGE_READ_SUCCESS) {
                        // TODO
                        System.out.println(PROMPT + "Status: " + message.getStatus() +
                                "\n" + PROMPT + "keyrange: " + message.getKey());
                    } else if (message.getStatus() == StatusType.SERVER_STOPPED) {
                        System.out.println(PROMPT + "SORRY! SERVER STATUS: " + message.getStatus());
                    }
                } catch (Exception e) {
                    logger.error(e);
                }
            } else {
                printError("Not connected");
            }
        }
        // create table <table name> <column names>
        else if (tokens[0].equals("create") && tokens[1].equals("table")) {
            String tableName = tokens[2];
            String columns = tokens[3];
            try {
                kvStore.CreateTable(tableName, columns);
            } catch (Exception e) {
                logger.error(e);
            }
        }
        // insert into <table name> values <values>
        else if (tokens[0].equals("insert") && tokens[1].equals("into") && tokens[3].equals("values")) {
            String tableName = tokens[2];
            String columns = tokens[4];
            try {
                kvStore.insertTableValues(tableName, columns);
            } catch (Exception e) {
                logger.error(e);
            }
        }
        // select column1,column2,... from <table name> where <condition>
        else if (tokens[0].equals("select")) {
            handleSelect(tokens, cmdLine);
        }
        // delete from <table name> where <condition>
        else if (tokens[0].equals("delete")) {
            String tableName = tokens[2];
            String condition = cmdLine.split("where ")[1];
            try {
                KVMessage response = kvStore.deleteQuery(tableName, condition);
            } catch (Exception e) {
                logger.error(e);
            }
        }
        // update <table name> set column1=data1,column2=data2 where <condition>
        else if (tokens[0].equals("update")) {
            String tableName = tokens[1];
            String updates = tokens[3];
            String condition = cmdLine.split("where ")[1];
            try {
                KVMessage response = kvStore.updateQuery(tableName, updates, condition);
            } catch (Exception e) {
                logger.error(e);
            }
        }
        else if(tokens[0].equals("disconnect")) {
            kvStore.disconnect();

        } else if(tokens[0].equals("logLevel")) {
            if(tokens.length == 2) {
                String level = setLevel(tokens[1]);
                if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
                    printError("No valid log level!");
                    printPossibleLogLevels();
                } else {
                    System.out.println(PROMPT +
                            "Log level changed to level " + level);
                }
            } else {
                printError("Invalid number of parameters for disconnect!");
            }

        } else if(tokens[0].equals("help")) {
            printHelp();
        } else {
            printError("Unknown command");
            printHelp();
        }
    }

    private void printQueryResult(String columns, String result) {
        try {
            String[] data = result.split(";");
            for (int i = 0; i < data.length; i++) {
                String[] sub = data[i].split(",");
                System.out.printf("|");
                for (int j = 0; j < sub.length; j++) {
                    System.out.printf("%8s|", sub[j]);
                }
                System.out.println();
                System.out.printf("-");
                for (int j = 0; j < sub.length; j++) {
                    System.out.printf("---------");
                }
                System.out.println();
            }
        } catch (Exception e) {
            System.out.println("Query result is empty");
        }
    }

    private void handleSelect(String[] tokens, String cmdLine) {
        String columns = tokens[1];
        String tableName;
        int last_where_index = -1;
        ArrayList<String> conditions = new ArrayList<>();
        if (cmdLine.contains("where")) {
            last_where_index = cmdLine.lastIndexOf("where ");
            System.out.println(last_where_index);
            conditions.add(cmdLine.substring(last_where_index + 6));
        }
        else {
            conditions.add("all");
        }
        if (last_where_index == -1) {
            tableName = cmdLine.substring(cmdLine.indexOf("from ") + 5);
        }
        else {
            tableName = cmdLine.substring(cmdLine.indexOf("from ") + 5, last_where_index - 1);
        }

        while (tableName.contains(" ")) {
            String s = tableName.substring(1, tableName.length() - 1);

            if (s.contains("where")) {
                last_where_index = s.lastIndexOf("where ");
                System.out.println(last_where_index);
                conditions.add(s.substring(last_where_index + 6));
            }
            if (last_where_index == -1) {
                tableName = s.substring(s.indexOf("from ") + 5);
            }
            else {
                tableName = s.substring(s.indexOf("from ") + 5, last_where_index - 1);
            }
        }
        try {
            KVMessage response = kvStore.selectQuery(tableName, columns, String.join("&&", conditions));
            printQueryResult(columns, response.getValue());
        } catch (Exception e) {
            logger.error(e);
        }
    }

    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("KV CLIENT HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("connect <address> <port>");
        sb.append("\t establishes a connection to a server\n");
        sb.append(PROMPT).append("disconnect");
        sb.append("\t\t\t disconnects from the server \n");

        sb.append(PROMPT).append("put <key> <value>");
        sb.append("\t\t inserts, updates, or deletes a key-value pair to the server \n");

        sb.append(PROMPT).append("get <key>");
        sb.append("\t\t\t retrieves the value for the given key from the storage server \n");

        sb.append(PROMPT).append("keyrange");
        sb.append("\t\t\t shows key range of the KVServers \n");

        sb.append(PROMPT).append("logLevel <level>");
        sb.append("\t\t sets the logger to the specified log level \n");
        sb.append(PROMPT).append("\t\t\t\t ");
        sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

        sb.append(PROMPT).append("quit ");
        sb.append("\t\t\t\t exits the program");
        System.out.println(sb.toString());
    }

    private void printPossibleLogLevels() {
        System.out.println(PROMPT
                + "Possible log levels are:");
        System.out.println(PROMPT
                + "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
    }

    private String setLevel(String levelString) {
        if(levelString.equals(Level.ALL.toString())) {
            logger.setLevel(Level.ALL);
            return Level.ALL.toString();
        } else if(levelString.equals(Level.DEBUG.toString())) {
            logger.setLevel(Level.DEBUG);
            return Level.DEBUG.toString();
        } else if(levelString.equals(Level.INFO.toString())) {
            logger.setLevel(Level.INFO);
            return Level.INFO.toString();
        } else if(levelString.equals(Level.WARN.toString())) {
            logger.setLevel(Level.WARN);
            return Level.WARN.toString();
        } else if(levelString.equals(Level.ERROR.toString())) {
            logger.setLevel(Level.ERROR);
            return Level.ERROR.toString();
        } else if(levelString.equals(Level.FATAL.toString())) {
            logger.setLevel(Level.FATAL);
            return Level.FATAL.toString();
        } else if(levelString.equals(Level.OFF.toString())) {
            logger.setLevel(Level.OFF);
            return Level.OFF.toString();
        } else {
            return LogSetup.UNKNOWN_LEVEL;
        }
    }

    private void printError(String error) {
        System.out.println(PROMPT + "Error! " +  error);
    }

    /**
     * Main entry point for the echo client application.
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
        try {
            new LogSetup("logs/client.log", Level.ALL);
            KVClient kvclient = new KVClient();
            kvclient.run();
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }
}