package app_kvServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.AccessibleObject;
import java.math.BigInteger;
import java.net.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;

import database.DatabaseManager;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.*;
import shared.messages.KVMessage;
import shared.messages.KVMessageEntity;

/**
 * Represents a Server that support put and get implementation.
 */
public class KVServer extends Thread implements IKVServer {
	private static Logger logger = Logger.getLogger(KVServer.class);

	private final int port;
	private final int cacheSize;
	private String strategy;
	private ServerSocket serverSocket;
	private boolean running;
	private final DatabaseManager dbManager;
	private String address = "localhost";
	private String name;

	/*M2*/
	private String ecsAddress = null;
	private CommunicationModule ecsCommunication;
	private Socket ecsSocket;
	private Metadata serverMetadata;
	private HashRange responsibleRange = new HashRange();
	private ServerStatus serverStatus;
	Thread ecsThread;
	ArrayList<String> transfer_keys = new ArrayList<>();

	/*M3*/
	private List<CommunicationModule> successors = Collections.synchronizedList(
			new ArrayList<>(0)); // coordinator connect with replica
	private HashRange replicaRange = new HashRange();

	/*M4*/
	private String queryDelimiter = ";";
	private String tableDelimiter = "@";
	private String columnDelimiter = ",";
	private TableManager tableManager;


	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache
	 *           is full and there is a GET- or PUT-request on a key that is
	 *           currently not contained in the cache. Options are "FIFO", "LRU",
	 *           and "LFU".
	 */

	public KVServer(int port, int cacheSize, String strategy, String dir, String name, String address,
					String ecsAddress) {
		this.port = port;
		this.cacheSize = cacheSize;
		this.strategy = strategy;
		this.dbManager = new DatabaseManager(dir, name);
		this.tableManager = new TableManager(this.dbManager);
		this.address = address;
		this.name = this.address + ":" + Integer.toString(port);
		this.ecsAddress = ecsAddress;

		if (ecsAddress == null) {
			setServerStatus(ServerStatus.ACTIVE);
		} else {
			setServerStatus(ServerStatus.STOP);
		}
//		new Thread(this).start();

		Runtime.getRuntime().addShutdownHook(new ShutDownHook());
	}

	public KVServer(int port, int cacheSize, String strategy) {
		this.port = port;
		this.cacheSize = cacheSize;
		this.strategy = strategy;
		this.dbManager = new DatabaseManager("disk", "database");

		if (ecsAddress == null) {
			this.serverStatus = ServerStatus.ACTIVE;
		} else {
			this.serverStatus = ServerStatus.STOP;
		}
//		new Thread(this).start();
	}

	@Override
	public int getPort(){
		return this.port;
	}

	@Override
	public String getHostname(){
		if (serverSocket != null)
			return serverSocket.getInetAddress().getHostName();
		else
			return null;
	}

	@Override
	public CacheStrategy getCacheStrategy(){
		return IKVServer.CacheStrategy.None; //incomplete
	}

	@Override
	public int getCacheSize(){
		return cacheSize;
	}

	@Override
	public boolean inStorage(String key){
		try {
			return dbManager.get(key) != null;
		} catch (Exception e) {
			return false;
		}
	}

	@Override
	public boolean inCache(String key){
		return false;
	}

	@Override
	public String getKV(String key) throws Exception{
		return dbManager.get(key);
	}

	@Override
	public void putKV(String key, String value) throws Exception {
		int i = 1;
		if (value == null || value.equals("null")) {
			dbManager.delete(key);

			for (CommunicationModule module: successors) {
				module.sendKVMessage(KVMessage.StatusType.REPLICA_DELETE, key, value);
				KVMessage response = module.receiveKVMessage();

				if (response.getStatus() != KVMessage.StatusType.DELETE_SUCCESS) {
					logger.warn("Delete-update Replica Failed");
				}
				else {
					logger.info(String.format("Delete-update replica at (%d)", i));
				}
				i += 1;
			}
		}
		else {
			dbManager.put(key, value);

			for (CommunicationModule module: successors) {
				module.sendKVMessage(KVMessage.StatusType.REPLICA_PUT, key, value);
				KVMessage response = module.receiveKVMessage();

				if (response.getStatus() != KVMessage.StatusType.PUT_SUCCESS &&
						response.getStatus() != KVMessage.StatusType.PUT_UPDATE) {
					logger.warn("Put-update Replica Failed");
				}
				else {
					logger.info(String.format("Put-update replica at (%d)", i));
				}
				i += 1;
			}
		}
	}

	public void putKVReplica(String key, String value) throws Exception {
		if (value == null || value.equals("null")) {
			dbManager.delete(key);
		}
		else {
			dbManager.put(key, value);
		}
	}

	@Override
	public void clearCache(){
		// TODO Auto-generated method stub
	}

	@Override
	public void clearStorage(){
		dbManager.eraseDisk();
	}

	private boolean initializeServer() {
		logger.info("Initialize server ...");
		try {
			InetAddress inetAddress = InetAddress.getByName(this.address);
			serverSocket = new ServerSocket(port, 50, inetAddress);
			logger.info("Server listening on " + address + ":" + port);
			return true;

		} catch (IOException e) {
			logger.error("Error! Cannot open server socket:");
			if(e instanceof BindException){
				logger.error("Port " + port + " is already bound!");
			}
			return false;
		}
	}

	@Override
	public void run(){
		running = initializeServer();

		if (this.ecsAddress != null) {
			connect2ECS();
		}

		if(serverSocket != null) {
			while(isRunning()){
				try {
					Socket client = serverSocket.accept();
					CommunicationModule communicationModule = new CommunicationModule(client, this);
					Thread thread = new Thread(communicationModule);
					thread.start();

					logger.info("Connected to "
							+ client.getInetAddress().getHostAddress()
							+  " on port " + client.getPort());

					communicationModule.sendKVMessage(KVMessage.StatusType.CONNECTED, "status", "successful");
				} catch (IOException e) {
					logger.error("Error! " +
							"Unable to establish connection. \n", e);
				}
			}
		}
		logger.info("Server stopped.");
	}

	private boolean isRunning(){
		return this.running;
	}

	@Override
	public void kill(){
		running = false;
		try {
			serverSocket.close();
		} catch (IOException e) {
			logger.error("Error! " +
					"Unable to close socket on port: " + port, e);
		}
	}

	@Override
	public void close(){
		// TODO Auto-generated method stub
		running = false;
		try {
			serverSocket.close();
		} catch (IOException e) {
			logger.error("Error! " +
					"Unable to close socket on port: " + port, e);
		}
	}

	/* M2 */
	public String getEcsAddress() {
		return ecsAddress;
	}
	public String getServerMetadata(){
		return this.serverMetadata.toString(false);
	}
	public void setEcsAddress(String ecsAddress) {
		this.ecsAddress = ecsAddress;
	}

	public String getECSHostname() {
		if (ecsAddress == null) {
			logger.warn("ECS address is null");
			return null;
		}
		return ecsAddress.split(":")[0];
	}

	public int getECSPort() {
		if (ecsAddress == null) {
			logger.warn("ECS address is null");
			return 0;
		}
		return Integer.parseInt(ecsAddress.split(":")[1]);
	}

	public ServerStatus getServerStatus() {
		return serverStatus;
	}

	public void setServerStatus(String serverStatus) {
		this.serverStatus = ServerStatus.valueOf(serverStatus);
		logger.info("Server Status is set to " + serverStatus);

		removeTransferKeyIfNeeded();
	}

	public void setServerStatus(ServerStatus serverStatus) {
		this.serverStatus = serverStatus;
		logger.info("Server Status is set to " + serverStatus.toString());

		removeTransferKeyIfNeeded();
	}

	private void removeTransferKeyIfNeeded() {
		if (!transfer_keys.isEmpty()) {
			for (String key: transfer_keys) {
				dbManager.delete(key);
			}

			transfer_keys = new ArrayList<>();
		}
	}

	private void connect2ECS() {
		String[] tokens = ecsAddress.split(":");
		int ecsPort;
		String ecsHostname = "localhost";
		if (tokens.length == 1) {
			ecsPort = Integer.parseInt(tokens[0]);
		}
		else {
			ecsHostname = tokens[0];
			ecsPort = Integer.parseInt(tokens[1]);
		}
		try {
			ecsSocket = new Socket(ecsHostname, ecsPort);

			ecsCommunication = new ECSCommunicationModule(ecsSocket, this);
			ecsCommunication.sendKVMessage(KVMessage.StatusType.ECS_CONNECT, this.address, String.valueOf(this.port));
			ecsCommunication.receiveKVMessage();

			ecsThread = new Thread(ecsCommunication);
			ecsThread.start();
		} catch (Exception e) {
			logger.error("Server cannot connect to ECS!");
			return;
		}
	}

	public boolean updateMetadata(String data) {
		this.serverMetadata = new Metadata(data);
		HashRange range = this.serverMetadata.getHashRange(this.name);

		if (range == null) {
			logger.error(String.format("Server [%s] cannot update its range because range is not in metadata",
					this.name));
			return false;
		}
		this.responsibleRange = range;

		logger.info(String.format("Server [%s] update its responsible range to (%s, %s)", this.name,
				this.responsibleRange.getStartIndex().toString(16),
				this.responsibleRange.getEndIndex().toString(16)));

		for (CommunicationModule module: this.successors) {
			module.closeCommunicationModule();
		}

		this.successors = Collections.synchronizedList(
				new ArrayList<>(0));

		if (serverMetadata.getSize() == 1) {
			this.replicaRange = this.responsibleRange;
		}

		Pair<String, HashRange> fstSuccessor = null;
		String[] fstAddress;

		if (serverMetadata.getSize() >= 2) {
			fstSuccessor = serverMetadata.getSuccessorAt(this.name, 1);

			fstAddress = fstSuccessor.getFirst().split(":");

			try {
				Socket fstSocket = new Socket(fstAddress[0], Integer.parseInt(fstAddress[1]));

				CommunicationModule fstComm = new CommunicationModule(fstSocket, null);
				fstComm.receiveKVMessage();
				this.successors.add(fstComm);
			} catch (Exception e) {
				logger.error(String.format("Coordinator [%s] connect to replica failed", this.name));
			}

			this.replicaRange.setStartIndex(serverMetadata.getSuccessorAt(this.name, -1).getSecond().getStartIndex());
			this.replicaRange.setEndIndex(this.responsibleRange.getEndIndex());

			logger.info(String.format("Coordinator [%s] connect to replica [%s]!", this.name, fstSuccessor.getFirst()));
		}

		if (serverMetadata.getSize() >= 3) {
			Pair<String, HashRange> sndSuccessor = serverMetadata.getSuccessorAt(this.name, 2);

			String[] sndAddress = sndSuccessor.getFirst().split(":");

			try {
				Socket sndSocket = new Socket(sndAddress[0], Integer.parseInt(sndAddress[1]));

				CommunicationModule sndComm = new CommunicationModule(sndSocket, null);
				sndComm.receiveKVMessage();
				this.successors.add(sndComm);
			} catch (Exception e) {
				logger.error(String.format("Coordinator [%s] connect to replica failed", this.name));
			}

			this.replicaRange.setStartIndex(serverMetadata.getSuccessorAt(this.name, -2).getSecond().getStartIndex());
			this.replicaRange.setEndIndex(this.responsibleRange.getEndIndex());

			logger.info(String.format("Coordinator [%s] connect to replica [%s]!", this.name, sndSuccessor.getFirst()));
		}

		logger.info(String.format("Server [%s] update its replica range to (%s, %s)", this.name,
				this.replicaRange.getStartIndex().toString(16),
				this.replicaRange.getEndIndex().toString(16)));

		return true;
	}

	public boolean rebalance(String dstName, String range) {
		logger.info(String.format("Server [%s] start rebalance, transfer key in range [%s] to server [%s].",
				this.name, range, dstName));


		// before rebalance, set to WRITE_LOCK
		setServerStatus(ServerStatus.WRITE_LOCK);

		// TODO: transfer data process
		String[] tokens = dstName.split(":");

		String dst_address = tokens[0];
		int dst_port = Integer.parseInt(tokens[1]);
		HashRange transfer_range = new HashRange(range);

		Set<String> all_keys = dbManager.getALlKeys();
		this.transfer_keys = new ArrayList<>();

		try {
			Socket socket = new Socket(dst_address, dst_port);

			// server to server
			CommunicationModule communicationModule = new CommunicationModule(socket, null);
			communicationModule.receiveKVMessage();

			for (String key: all_keys) {
				BigInteger keyHash = MD5Generator.generateHash(key);
				if (transfer_range.isInRange(keyHash)) {
					this.transfer_keys.add(key);
				}
			}

			for (String key: transfer_keys) {
				String value = dbManager.get(key);

				communicationModule.sendKVMessage(KVMessage.StatusType.SERVER_PUT, key, value);
				KVMessage reply = communicationModule.receiveKVMessage();
				if (reply.getStatus() != KVMessage.StatusType.PUT_SUCCESS &&
						reply.getStatus() != KVMessage.StatusType.PUT_UPDATE) {
					logger.error("UNEXPECTED ERROR IN REBALANCE DATA!");
				}
			}

			communicationModule.closeCommunicationModule();
		} catch (Exception e) {
			logger.error("REBALANCE DATA FAILED");
			return false;
		}

		// after rebalance, set to ACTIVE or STOP depends on whether it is going to shutdown
//		if (isRunning()) {
//			setServerStatus(ServerStatus.ACTIVE);
//		}
//		else {
//			setServerStatus(ServerStatus.STOP);
//		}

		return true;
	}

	public boolean transferKVPairs(String dstName, String range) {
		logger.info(String.format("Server [%s] start transfer key in range [%s] to server [%s].",
				this.name, range, dstName));

		setServerStatus(ServerStatus.WRITE_LOCK);

		String[] tokens = dstName.split(":");

		String dst_address = tokens[0];
		int dst_port = Integer.parseInt(tokens[1]);
		HashRange transfer_range = new HashRange(range);

		Set<String> all_keys = dbManager.getALlKeys();

		try {

			Socket socket = new Socket(dst_address, dst_port);

			// server to server
			CommunicationModule communicationModule = new CommunicationModule(socket, null);
			communicationModule.receiveKVMessage();

			for (String key: all_keys) {
				BigInteger keyHash = MD5Generator.generateHash(key);
				if (transfer_range.isInRange(keyHash)) {
					String value = dbManager.get(key);

					communicationModule.sendKVMessage(KVMessage.StatusType.SERVER_PUT, key, value);
					KVMessage reply = communicationModule.receiveKVMessage();
					if (reply.getStatus() != KVMessage.StatusType.PUT_SUCCESS &&
							reply.getStatus() != KVMessage.StatusType.PUT_UPDATE) {
						logger.error("UNEXPECTED ERROR IN TRANSFER DATA!");
					}
				}
			}

			communicationModule.closeCommunicationModule();
		} catch (Exception e) {
			logger.error("TRANSFER DATA FAILED");
			return false;
		}

		setServerStatus(ServerStatus.ACTIVE);

		return true;
	}

	public void deleteKVPairs(String range) {
		HashRange transfer_range = new HashRange(range);

		logger.info("Delete all data in range " + range);

		Set<String> all_keys = dbManager.getALlKeys();

		for (String key: all_keys) {
			BigInteger keyHash = MD5Generator.generateHash(key);
			if (transfer_range.isInRange(keyHash)) {
				this.dbManager.delete(key);
			}
		}
	}

	public class ShutDownHook extends Thread {

		public void run(){
			shutdown();
		}
	}

	public void runShutDownHook() {
		new ShutDownHook().start();
	}

	public void shutdown() {
		if (!isRunning()) {
			return;
		}

		if (this.serverMetadata == null) {
			return;
		}

		this.running = false;

		logger.info(String.format("Server [%s] is shutting down", this.name));

		if (this.serverMetadata.getSize() == 1) {
			logger.debug("Last node in ECS");
		} else {
			logger.debug("Deleting all keys from db");
			dbManager.eraseDisk();
		}
	}

	public boolean isInCoordinatorRange(String key) {
		BigInteger keyHash = MD5Generator.generateHash(key);
		return this.responsibleRange.isInRange(keyHash);
	}

	public boolean isInReplicaRange(String key) {
		BigInteger keyHash = MD5Generator.generateHash(key);
		return this.replicaRange.isInRange(keyHash);
	}

	public HashRange getResponsibleRange() {
		return responsibleRange;
	}

	/*M3*/

	public String getServerMetadataWithReplica(){
		return this.serverMetadata.toString(true);
	}

	/*M4*/

	public String selectQuery(String tableName, String s) {
		return tableManager.selectQuery(tableName, s);
	}

	public boolean deleteQuery(String tableName, String condition) {
		// keys of KV-pairs that need to be deleted
		ArrayList<String> toBeDeleted = tableManager.deleteQuery(tableName, condition);

		if (toBeDeleted == null) return true;

		try {
			for (String s : toBeDeleted) {
				putKV(s, null);
			}
		} catch (Exception e) {
			logger.error("Delete query erorr", e);
			return false;
		}

		return true;
	}

	public boolean updateQuery(String tableName, String s) {
		// map of keys of KV-pairs that need to be updated and updated value s
		Map<String, String> toBeUpdated = tableManager.updateQuery(tableName, s);

		if (toBeUpdated == null) return true;

		try {
			for (String key : toBeUpdated.keySet()) {
				putKV(key, toBeUpdated.get(key));
			}
		} catch (Exception e) {
			logger.error("Delete query erorr", e);
			return false;
		}

		return true;
	}

	public static void main(String[] args) {
		int port=-1;
		int cacheSize = 100;
		String strategy = "unkwown";
		String address = "localhost";
		String dir = "disk";
		String logdir = "logs/server.log";
		Level logLevel = Level.ALL;
		String ECSAddress = null;
		try {
			for (int i = 0; i < args.length; i++) {
				switch (args[i]) {
					case "-p":
						port = Integer.parseInt(args[++i]);
						break;
					case "-a":
						address = args[++i];
						break;
					case "-d":
						dir = args[++i];
						break;
					case "-l":
						logdir = args[++i];
						break;
					case "-b":
						ECSAddress = args[++i];
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

			String dbName = "database_" + address + "_" + port;
			KVServer server = new KVServer(port, cacheSize, strategy, dir, dbName, address, ECSAddress);

			server.start();
		} catch (IOException e) {
			logger.error("Error! Unable to initialize logger!");
			System.exit(1);
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port>! Not a number!");
			System.out.println(port);
			System.out.println("Usage: Server <port>");
			System.exit(1);
		}
	}
}