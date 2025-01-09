package testing;

import org.junit.Test;

import client.KVStore;
import junit.framework.TestCase;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;

public class AdditionalTest extends TestCase {
	
	private KVStore kvClient;
	private KVStore kvClient_test;

	public void setUp() {
		kvClient = new KVStore("localhost", 50000);
		kvClient_test = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
			kvClient_test.connect();
		} catch (Exception e) {
		}
	}

	public void tearDown() {
		kvClient.disconnect();
		kvClient_test.disconnect();
	}

	@Test
	// Test1: test length contraints of put key
	public void testPutLength() {
		String key = "iiiiiiiiiiammmmmmmmmmwayyyyyyyyyyytooooooooolonggggggg";
		String value = "iamwaytoolong";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertNotNull(ex);
	}

	@Test
	// Test2: test length contraints of get key
	public void testGetLength() {
		String key = "iiiiiiiiiiammmmmmmmmmwayyyyyyyyyyytooooooooolonggggggg";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}

		assertNotNull(ex);
	}

	@Test
	// Test3: one client processes a series of commands
	public void testPutGet() {
		String key1 = "foo11";
		String value1 = "bar11";
		String key2 = "foo22";
		String value2 = "bar22";
		String key3 = "foo33";
		KVMessage response1 = null;
		KVMessage response2 = null;
		KVMessage response3 = null;
		KVMessage response4 = null;
		Exception ex = null;

		try {
			response1 = kvClient.put(key1, value1);
			response2 = kvClient.get(key1);
			response3 = kvClient.put(key2, value2);
			response4 = kvClient.get(key3);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response1.getStatus() == StatusType.PUT_SUCCESS 
				&& response3.getStatus() == StatusType.PUT_SUCCESS
				&& response2.getStatus() == StatusType.GET_SUCCESS 
				&& response4.getStatus() == StatusType.GET_ERROR);
	}

	@Test
	// Test 4: test random string
	public void testRandomString() {
		String key = "a%*&$./?,<>Aa_-=+m";
		String value = "i_am_a_weird_string";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
	}

	@Test
	// Test 5: multiple client interact with one server
	public void testMultiClient() {
		String key1 = "foo14";
		String value1 = "bar14";
		String key2 = "foo24";
		String value2 = "bar24";
		KVMessage response1 = null;
		KVMessage response2 = null;
		KVMessage response3 = null;
		KVMessage response4 = null;
		Exception ex = null;

		try {
			response1 = kvClient.put(key1, value1);
			response2 = kvClient.get(key1);
			response3 = kvClient_test.put(key2, value2);
			response4 = kvClient_test.get(key2);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response1.getStatus() == StatusType.PUT_SUCCESS
				   && response2.getStatus() == StatusType.GET_SUCCESS
				   && response3.getStatus() == StatusType.PUT_SUCCESS
				   && response4.getStatus() == StatusType.GET_SUCCESS);
	}

	@Test
	// Test 6: delete the same key for twice
	public void testDoubleDelete() {
		String key = "deleteTestValue1";
		String value = "toDelete1";
		
		KVMessage response1 = null;
		KVMessage response2 = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			response1 = kvClient.put(key, "null");
			response2 = kvClient.put(key, "null");
			
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null && response1.getStatus() == StatusType.DELETE_SUCCESS
							  && response2.getStatus() == StatusType.DELETE_ERROR);
	}

	@Test
	// Test 7: test write to disk before connection and read from disk after re-connection
	public void testCrossConnection() {
		String key = "before_disconnect";
		String value = "before_disconnect";
		KVMessage response1 = null;
		KVMessage response2 = null;
		Exception ex = null;

		try {
			response1 = kvClient.put(key, value);
			kvClient.disconnect();
			kvClient.connect();
			response2 = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response1.getStatus() == StatusType.PUT_SUCCESS
							  && response2.getStatus() == StatusType.GET_SUCCESS);
	}

	@Test
	// Test 8: test write to disk from client1 and read to client2
	public void testCrossClient() {
		String key = "message_from_1";
		String value = "message_from_1";
		KVMessage response1 = null;
		KVMessage response2 = null;
		Exception ex = null;

		try {
			response1 = kvClient.put(key, value);
			response2 = kvClient_test.get(key);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response1.getStatus() == StatusType.PUT_SUCCESS
							  && response2.getStatus() == StatusType.GET_SUCCESS);
	}

	@Test
	// Test 9: test message sending cross connections and clients
	public void testCrossClientAndConnection() {
		String key1 = "message_1";
		String value1 = "message_1";
		String key2 = "message_2";
		String value2 = "message_2";
		KVMessage response1 = null;
		KVMessage response2 = null;
		KVMessage response3 = null;
		KVMessage response4 = null;
		Exception ex = null;

		try {
			response1 = kvClient.put(key1, value1);
			response2 = kvClient_test.put(key2, value2);
			kvClient.disconnect();
			response3 = kvClient_test.get(key1);
			kvClient.connect();
			response4 = kvClient.get(key2);

		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response1.getStatus() == StatusType.PUT_SUCCESS
							  && response2.getStatus() == StatusType.PUT_SUCCESS
							  && response3.getStatus() == StatusType.GET_SUCCESS
							  && response4.getStatus() == StatusType.GET_SUCCESS);
	}

	@Test
	// Test10: test various PUT states
	public void testPutStates() {
		String key1 = "i_am_the_key";
		String value1 = "i_should_be_success";
		String value2 = "i_should_be_update";
		String value3 = "i_should_also_be_success";
		KVMessage response1 = null;
		KVMessage response2 = null;
		KVMessage response3 = null;
		KVMessage response4 = null;
		Exception ex = null;

		try {
			response1 = kvClient.put(key1, value1);
			response2 = kvClient_test.put(key1, value2);
			response3 = kvClient.put(key1, null);
			response4 = kvClient_test.put(key1, value3);
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null && response1.getStatus() == StatusType.PUT_SUCCESS 
				&& response2.getStatus() == StatusType.PUT_UPDATE
				&& response3.getStatus() == StatusType.DELETE_SUCCESS 
				&& response4.getStatus() == StatusType.PUT_SUCCESS);
	}
}
