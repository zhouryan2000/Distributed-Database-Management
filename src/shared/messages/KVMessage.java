package shared.messages;

public interface KVMessage {
	
	public enum StatusType {
		GET, 			/* Get - request */
		GET_ERROR, 		/* requested tuple (i.e. value) not found */
		GET_SUCCESS, 	/* requested tuple (i.e. value) found */
		PUT, 			/* Put - request */
		PUT_SUCCESS, 	/* Put - request successful, tuple inserted */
		PUT_UPDATE, 	/* Put - request successful, i.e. value updated */
		PUT_ERROR, 		/* Put - request not successful */
		DELETE_SUCCESS, /* Delete - request successful */
		DELETE_ERROR,	/* Delete - request successful */
		CONNECTED,
		FAILED,
		UNKNOWN, /* Unknown status */

		/* M2 Server communication to client */
		SERVER_STOPPED, /* Server is stopped, no requests are processed */
		SERVER_WRITE_LOCK, /* Server locked for write, only get possible */
		SERVER_NOT_RESPONSIBLE, /* Request not successful, server not responsible for key */
		KEYRANGE,		/* Request keyrange from server */
		/*KEYRANGE_SUCCESS serverMetadata null*/
		KEYRANGE_SUCCESS,
		/* M2 ECS communication with Server */
		ECS_CONNECT, 	/* Server wants to connect to ECS: ECS_CONNECT hostname port*/
		ECS_CONNECT_SUCCESS,
		SHUTDOWN, 	/*Server tells ECS*/
		UPDATE_SERVER_METADATA, /* ECS ask servers to update metadata */
		UPDATE_SERVER_METADATA_SUCCESS,
		/* REBALANCE dstNode range */
		REBALANCE, 		/*ECS sends to servers to initiate rebalancing */
		REBALANCE_SUCCESS,
		REBALANCE_ERROR,
		UPDATE_SERVER_STATUS,
		UPDATE_SERVER_STATUS_SUCCESS,

		/* M2 Server to Server */
		SERVER_PUT,

		/*M3*/
		KEYRANGE_READ,
		KEYRANGE_READ_SUCCESS,
		IS_ALIVE, /*heartbeat message*/
		ALIVE,
		REPLICA_DELETE,
		REPLICA_PUT,
		TRANSFER_DATA,
		TRANSFER_DATA_SUCCESS,
		TRANSFER_DATA_ERROR,
		DELETE_DATA,
		DELETE_DATA_SUCCESS,

		/*M4*/
		SELECT_QUERY,
		UPDATE_QUERY,
		DELETE_QUERY,
		QUERY_SUCCESS,
		QUERY_ERROR,
	}

	/**
	 * @return the key that is associated with this message, 
	 * 		null if not key is associated.
	 */
	public String getKey();
	
	/**
	 * @return the value that is associated with this message, 
	 * 		null if not value is associated.
	 */
	public String getValue();
	
	/**
	 * @return a status string that is used to identify request types, 
	 * response types and error types associated to the message.
	 */
	public StatusType getStatus();

	public void setStatus(StatusType status);

	public void setKey(String key);

	public void setValue(String value);

	public String outputFormat();

	public byte[] toByteArray();

}


