package client;

import org.apache.log4j.Logger;
import shared.CommunicationModule;
import shared.Metadata;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;
import shared.messages.KVMessageEntity;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;

public class KVStore implements client.KVCommInterface {
	private Socket clientSocket;
	private String address;
	private int port;
	private OutputStream output;
	private InputStream input;

	private final Logger logger = Logger.getLogger(KVStore.class);
	private boolean running;

	private Metadata metadata;

	private CommunicationModule communicationModule;

	private String tableDelimiter = "@";
	private String queryDelimiter = ";";


	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		this.address = address;
		this.port = port;
		this.metadata = new Metadata();
		this.metadata.addServer(address, port);
		setRunning(true);
	}

	@Override
	public void connect() throws Exception {
		clientSocket = new Socket(address, port);
		try {
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
			communicationModule = new CommunicationModule(clientSocket, null); // null
			communicationModule.receiveKVMessage();
		} catch (Exception e) {
		}
		setRunning(true);
	}

	@Override
	public void disconnect() {
		try {
			tearDownConnection();
		} catch (IOException e) {
			logger.error("Unable to close connection!");
		}
	}

	private void tearDownConnection() throws IOException {
		setRunning(false);
		logger.info("tearing down the connection ...");
		if (clientSocket != null) {
			input.close();
			output.close();
			clientSocket.close();
			clientSocket = null;
			logger.info("connection closed!");
		}
	}

	private void handleServerShutdown() {
//		ArrayList<String> servers = this.metadata.getAllServer();
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		if (key.getBytes().length > 20) {
			logger.error("Key length exceeds 20 Bytes");
			throw new IOException("Error! Key length exceeds 20 Bytes");
		}
		if (key.getBytes().length > 122880) {
			logger.error("Value length exceeds 120 kBytes");
			throw new IOException("Error! Value length exceeds 120 kBytes");
		}

		KVMessage response = null;

		try {
			communicationModule.sendKVMessage(StatusType.PUT, key, value);
			response = communicationModule.receiveKVMessage();

			if (response.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE) {
				logger.info("Server Not Responsible");

				handleServerNotResponsible(key);

				response = put(key, value);
			}
		} catch (Exception e) {
			handleServerShutdown();

            throw new IOException("The server was shut down");
		}

		return response;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		if (key.getBytes().length > 20) {
			logger.error("Key length exceeds 20 Bytes");
			throw new IOException("Error! Key length exceeds 20 Bytes");
		}

		KVMessage response = null;

		try {
			communicationModule.sendKVMessage(StatusType.GET, key, null);
			response = communicationModule.receiveKVMessage();

			if (response.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE) {
				logger.info("Server Not Responsible");

				handleServerNotResponsible(key);

				response = get(key);
			}
		} catch (Exception e) {
			handleServerShutdown();

			response = new KVMessageEntity(StatusType.SERVER_STOPPED, "Failed", "Server was shut down");

			throw new IOException("The server was shut down");
		}

		return response;
	}

	// update the metadata
	private void handleServerNotResponsible(String key) throws Exception {

		KVMessage response = keyrange();
		String metadata_str = response.getKey();

		while (metadata_str == null) {
			response = keyrange();
			metadata_str = response.getKey();
		}

		this.metadata = new Metadata(metadata_str);

		logger.info("Metadata is updated to "+ this.metadata.toString());

		connectToResponsibleServer(key);
	}

//	public void getConnectToResponsdingServer(String key){
//		connectToResponsdingServer(key);
//	}

	public Metadata getMetadata(){
		return this.metadata;
	}

	private void connectToResponsibleServer(String key) {
		String resp = metadata.findResponsibleServer(key);

		logger.debug(String.format("The responsible server is %s, its hash range is [%s]", resp,
				this.metadata.getHashRange(resp)));

		String[] tokens = resp.split(":");

		String resp_address = tokens[0];
		int resp_port = Integer.parseInt(tokens[1]);

		if (!resp_address.equals(this.address) || resp_port != this.port) {

			this.address = resp_address;
			this.port = resp_port;

			try {
				disconnect();
				connect();
				logger.info(String.format("\n==============================\n" +
						"Client now connect to [%s]\n===========================\n", resp));
			} catch (Exception e) {
				logger.info(String.format("Connect to responsible server failed [%s] failed", resp));
			}
		}
	}

	public KVMessage keyrange() throws Exception {
		communicationModule.sendKVMessage(StatusType.KEYRANGE, null, null);
		return communicationModule.receiveKVMessage();
	}

	public KVMessage keyrangeRead() throws Exception {
		communicationModule.sendKVMessage(StatusType.KEYRANGE_READ, null, null);
		return communicationModule.receiveKVMessage();
	}

	public void setRunning(boolean running) {
		this.running = running;
	}

	public boolean isRunning() {
		return running;
	}

	public KVMessage CreateTable(String tableName, String columns) throws Exception {
		columns = columns.substring(1, columns.length() - 1);
		put(tableName + tableDelimiter + "columns", columns);
		return put(tableName + tableDelimiter + "id", "0");
	}

	public KVMessage insertTableValues(String tableName, String values) throws Exception {
		String s = tableName + tableDelimiter + "id";
		int id = Integer.parseInt(get(s).getValue()) + 1;
		put(s, String.valueOf(id));
		values = values.substring(1, values.length() - 1);
		return put(tableName + tableDelimiter + id, values);
	}

	public KVMessage selectQuery(String tableName, String columns, String condition) throws IOException {
		KVMessage response = null;

		try {
			communicationModule.sendKVMessage(StatusType.SELECT_QUERY, tableName, columns + queryDelimiter + condition);
			response = communicationModule.receiveKVMessage();

			if (response.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE) {
				logger.info("Server Not Responsible");

				handleServerNotResponsible(tableName);

				response = selectQuery(tableName, columns, condition);
			}
		} catch (Exception e) {
			handleServerShutdown();

			response = new KVMessageEntity(StatusType.SERVER_STOPPED, "Failed", "Server was shut down");

			throw new IOException("The server was shut down");
		}
		return response;
	}

	public KVMessage deleteQuery(String tableName, String condition) throws IOException {
		KVMessage response = null;

		try {
			communicationModule.sendKVMessage(StatusType.DELETE_QUERY, tableName, condition);
			response = communicationModule.receiveKVMessage();

			if (response.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE) {
				logger.info("Server Not Responsible");

				handleServerNotResponsible(tableName);

				response = deleteQuery(tableName, condition);
			}
		} catch (Exception e) {
			handleServerShutdown();

			response = new KVMessageEntity(StatusType.SERVER_STOPPED, "Failed", "Server was shut down");

			throw new IOException("The server was shut down");
		}
		return response;
	}

	public KVMessage updateQuery(String tableName, String updates, String condition) throws IOException {
		KVMessage response = null;

		try {
			communicationModule.sendKVMessage(StatusType.UPDATE_QUERY, tableName, updates + queryDelimiter + condition);
			response = communicationModule.receiveKVMessage();

			if (response.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE) {
				logger.info("Server Not Responsible");

				handleServerNotResponsible(tableName);

				response = updateQuery(tableName, updates, condition);
			}
		} catch (Exception e) {
			handleServerShutdown();

			response = new KVMessageEntity(StatusType.SERVER_STOPPED, "Failed", "Server was shut down");

			throw new IOException("The server was shut down");
		}
		return response;
	}
}
