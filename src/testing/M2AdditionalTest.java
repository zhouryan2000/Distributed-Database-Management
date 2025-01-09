package testing;

import app_kvECS.ECSClient;
import app_kvServer.KVServer;
import client.KVStore;
import ecs.ECSNode;
import junit.framework.TestCase;
import org.junit.Test;
import shared.HashRange;
import shared.messages.KVMessage;
import static shared.MD5Generator.generateHash;
import shared.messages.KVMessage.StatusType;

import java.math.BigInteger;
import java.util.HashMap;

public class M2AdditionalTest extends TestCase {
    public static ECSClient ecsClient;
    private static Thread ecsThread;
    private HashMap<String, KVServer> name2Servers = new HashMap<>();

    public void setUpECS(int port) {
        try {
            // initialize an ECSClient and run it on a new thread
            ecsThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    ecsClient = new ECSClient("localhost", port);
                    ecsClient.run();
                }
            });
            ecsThread.start();
        } catch (Exception e) {

        }
    }

//    @After
//    public void tearDown() throws Exception {
//        ecsThread.join(10);
//    }

    // initialize a server and run it on a new thread
    public KVServer createServer(String address, int port, String database, int ECSPort) {
        final KVServer[] servers = {new KVServer(port, 10, "FIFO", "disk", database,
                address, "localhost:"+ECSPort)};
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    servers[0].run();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail();
                }
            }
        }).start();

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        return servers[0];
    }

    @Test
    // Test ECS connection
    public void testConnectTOECS() {
        setUpECS(6000);

        KVServer kvServer1;
        Exception ex = null;
        ECSNode node = null;

        KVServer server = createServer("localhost", 5100, "test_data_set", 6000);
        name2Servers.put("localhost:5100", server);

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Is the server connected to ECS?
        assertNotNull(ecsClient.getNodes().get("localhost:5100"));

        server.runShutDownHook();

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

//        ecsClient.getRemoveNode(ecsClient.getNodes().get("localhost:5100"));

        assertNull(ecsClient.getNodes().get("localhost:5100"));
    }


    @Test
    // Test 2: Test Single Server/client Functionality
    public void testSingleServer() {
        setUpECS(6001);

        KVServer server20 = createServer("localhost", 5120, "test_data_set_20", 6001);
        name2Servers.put("localhost:5120", server20);
        KVStore kvClient20 = new KVStore("localhost", 5120);

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        try {
            kvClient20.connect();
            kvClient20.put("a", "b");
            kvClient20.get("a");
            kvClient20.disconnect();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {}


        HashRange range20 = server20.getResponsibleRange();

        assertTrue(range20.isInRange(generateHash("a")));
    }

    @Test
    // Test 3: Test Server Rebalancing
    public void testRebalance() {
        setUpECS(6002);

        KVServer server1 = createServer("localhost", 5101, "test_data_set_1", 6002);
        name2Servers.put("localhost:5101", server1);
        KVServer server2 = createServer("localhost", 5102, "test_data_set_2", 6002);
        name2Servers.put("localhost:5102", server2);
        KVServer server3 = createServer("localhost", 5103, "test_data_set_3", 6002);
        name2Servers.put("localhost:5103", server3);

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // confirm all servers are connected to ECS
        assertNotNull(ecsClient.getNodes().get("localhost:5101"));
        assertNotNull(ecsClient.getNodes().get("localhost:5102"));
        assertNotNull(ecsClient.getNodes().get("localhost:5103"));

        HashRange range1 = server1.getResponsibleRange();
        HashRange range2 = server2.getResponsibleRange();
        HashRange range3 = server3.getResponsibleRange();

        // confirm rebalance index correct
        assertTrue(range1.getEndIndex().equals(range2.getStartIndex().subtract(BigInteger.valueOf(1)))
                && range2.getEndIndex().equals(range3.getStartIndex().subtract(BigInteger.valueOf(1))));
    }

    @Test
    // Test 4: Test data will be transferred to correct server when notresponding state (re-connection case)
    public void testReConnect() {
        setUpECS(6004);

        KVServer server4 = createServer("localhost", 5104, "test_data_set_4", 6004);
        name2Servers.put("localhost:5104", server4);
        KVServer server5 = createServer("localhost", 5105, "test_data_set_5", 6004);
        name2Servers.put("localhost:5105", server5);
        KVStore kvClient5 = new KVStore("localhost", 5105);

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        try {
            kvClient5.connect();
            kvClient5.put("aa", "b");
            kvClient5.get("aa");
            kvClient5.disconnect();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {}

        HashRange range5 = server5.getResponsibleRange();

        assertTrue(range5.isInRange(generateHash("aa")));
    }

    @Test
    // Test 5: Test Server removing
    public void testRemoveServer() {
        setUpECS(6005);

        KVServer server6 = createServer("localhost", 5106, "test_data_set_6", 6005);
        name2Servers.put("localhost:5106", server6);
        KVServer server7 = createServer("localhost", 5107, "test_data_set_7", 6005);
        name2Servers.put("localhost:5107", server7);

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        HashRange range6_preremove = server6.getResponsibleRange();
        HashRange range7_preremove = server7.getResponsibleRange();

        assertTrue(range6_preremove.getEndIndex().equals(range7_preremove.getStartIndex().subtract(BigInteger.valueOf(1))));

        server6.runShutDownHook();

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        HashRange range7 = server7.getResponsibleRange();

        // confirm rebalance index correct
        assertTrue(range7.getEndIndex().equals(range7.getStartIndex().subtract(BigInteger.valueOf(1))));

//         server7.runShutDownHook();
    }

    @Test
    // Test 6: Test only one server left and all data will be transfer to it properly
    public void testLastOneStanding() {
        setUpECS(6006);

        KVServer server8 = createServer("localhost", 5108, "test_data_set_8", 6006);
        name2Servers.put("localhost:5108", server8);
        KVServer server9 = createServer("localhost", 5109, "test_data_set_9", 6006);
        name2Servers.put("localhost:5109", server9);
        KVServer server10 = createServer("localhost", 5110, "test_data_set_10", 6006);
        name2Servers.put("localhost:5110", server10);

        KVStore kvClient8 = new KVStore("localhost", 5108);
        KVStore kvClient9 = new KVStore("localhost", 5109);
        KVStore kvClient10 = new KVStore("localhost", 5110);

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        try {
            kvClient8.connect();
            kvClient8.put("eight", "8");
            kvClient8.get("eight");
            kvClient8.disconnect();
            kvClient9.connect();
            kvClient9.put("nine", "9");
            kvClient9.get("nine");
            kvClient9.disconnect();
            kvClient10.connect();
            kvClient10.put("ten", "10");
            kvClient10.get("ten");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {}

        server8.runShutDownHook();

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        server9.runShutDownHook();

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        KVMessage response1 = null;
        KVMessage response2 = null;
        KVMessage response3 = null;

        try {
            response1 = kvClient10.get("eight");
            response2 = kvClient10.get("nine");
            response3 = kvClient10.get("ten");
            kvClient10.disconnect();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {}

        server10.runShutDownHook();

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        assertTrue(response1.getStatus() == StatusType.GET_SUCCESS
                && response2.getStatus() == StatusType.GET_SUCCESS
                && response3.getStatus() == StatusType.GET_SUCCESS);
    }

    @Test
    // Test 7: Test persistency
    public void testPersistent() {
        setUpECS(6007);

        KVServer server11 = createServer("localhost", 5111, "test_data_set_11", 6007);
        name2Servers.put("localhost:5100", server11);
        KVServer server12 = createServer("localhost", 5112, "test_data_set_12", 6007);
        name2Servers.put("localhost:5100", server12);
        KVServer server13 = createServer("localhost", 5113, "test_data_set_13", 6007);
        name2Servers.put("localhost:5100", server13);

        KVStore kvClient11 = new KVStore("localhost", 5111);
        KVStore kvClient12 = new KVStore("localhost", 5112);
        KVStore kvClient13 = new KVStore("localhost", 5113);

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        try {
            kvClient11.connect();
            kvClient11.put("eleven", "11");
            kvClient11.disconnect();
            kvClient12.connect();
            kvClient12.put("twelve", "12");
            kvClient12.disconnect();
            kvClient13.connect();
            kvClient13.put("thirteen", "13");
            kvClient13.disconnect();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {}

        server11.runShutDownHook();

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        server12.runShutDownHook();

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        server13.runShutDownHook();

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        KVServer server13_ = name2Servers.get("localhost:5113");
        KVStore kvClient13_ = new KVStore("localhost", 5113);

        KVMessage response1 = null;
        KVMessage response2 = null;
        KVMessage response3 = null;

        try {
            kvClient13_.connect();
            response1 = kvClient13_.get("eleven");
            response2 = kvClient13_.get("twelve");
            response3 = kvClient13_.get("thirteen");
            kvClient13_.disconnect();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {}


        assertTrue(response1.getStatus() == StatusType.GET_SUCCESS
                && response2.getStatus() == StatusType.GET_SUCCESS
                && response3.getStatus() == StatusType.GET_SUCCESS);
    }

    @Test
    // Test 8: Test client reconnect metadata update
    public void testMetadataUpdate() {
        setUpECS(6008);

        KVServer server14 = createServer("localhost", 5114, "test_data_set_14", 6008);
        name2Servers.put("localhost:5114", server14);
        KVServer server15 = createServer("localhost", 5115, "test_data_set_15", 6008);
        name2Servers.put("localhost:5115", server15);
        KVStore kvClient15 = new KVStore("localhost", 5115);

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        try {
            kvClient15.connect();
            kvClient15.put("a", "b");
            kvClient15.disconnect();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {}

        server15.runShutDownHook();

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        assertTrue(kvClient15.getMetadata().server2rangeList.size() == 2);
        KVStore kvClient14 = new KVStore("localhost", 5114);

        try {
            kvClient14.connect();
            kvClient14.get("a");
            kvClient14.disconnect();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {}

        assertTrue(kvClient14.getMetadata().server2rangeList.size() == 1);
    }

    @Test
    // Test 9: Test Server removing, adding for multiple times
    public void testAddRemoveServer() {
        setUpECS(6009);

        KVServer server16 = createServer("localhost", 5116, "test_data_set_16", 6009);
        name2Servers.put("localhost:5116", server16);
        KVServer server17 = createServer("localhost", 5117, "test_data_set_17", 6009);
        name2Servers.put("localhost:5117", server17);
        KVStore kvClient16 = new KVStore("localhost", 5116);

        KVMessage response1 = null;
        KVMessage response2 = null;
        KVMessage response3 = null;
        KVMessage response4 = null;
        KVMessage response5 = null;
        KVMessage response6 = null;
        KVMessage response7 = null;
        KVMessage response8 = null;
        KVMessage response9 = null;
        KVMessage response10 = null;

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        try {
            kvClient16.connect();
            kvClient16.put("hello", "world");
            kvClient16.put("apple", "banana");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {}

        try {
            response1 = kvClient16.get("hello");
            response2 = kvClient16.get("apple");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {}

        KVServer server18 = createServer("localhost", 5118, "test_data_set_18", 6009);
        name2Servers.put("localhost:5118", server18);

        try {
            response3 = kvClient16.get("hello");
            response4 = kvClient16.get("apple");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {}

        server17.runShutDownHook();

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        try {
            response5 = kvClient16.get("hello");
            response6 = kvClient16.get("apple");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {}

        KVServer server19 = createServer("localhost", 5119, "test_data_set_19", 6009);
        name2Servers.put("localhost:5119", server19);

        try {
            response7 = kvClient16.get("hello");
            response8 = kvClient16.get("apple");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {}

        server18.runShutDownHook();

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        try {
            response9 = kvClient16.get("hello");
            response10 = kvClient16.get("apple");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {}

        assertTrue(response1.getStatus() == StatusType.GET_SUCCESS
                && response2.getStatus() == StatusType.GET_SUCCESS
                && response3.getStatus() == StatusType.GET_SUCCESS
                && response4.getStatus() == StatusType.GET_SUCCESS
                && response5.getStatus() == StatusType.GET_SUCCESS
                && response6.getStatus() == StatusType.GET_SUCCESS
                && response7.getStatus() == StatusType.GET_SUCCESS
                && response8.getStatus() == StatusType.GET_SUCCESS
                && response9.getStatus() == StatusType.GET_SUCCESS
                && response10.getStatus() == StatusType.GET_SUCCESS);
    }

    @Test
    // Test 10: test key range
    public void testKeyRange() {
        setUpECS(6010);

        KVServer server21 = createServer("localhost", 5121, "test_data_set_21", 6010);
        name2Servers.put("localhost:5121", server21);

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        HashRange range21 = server21.getResponsibleRange();

        assertTrue(range21.getStartIndex().equals(range21.getEndIndex().add(BigInteger.valueOf(1))));
    }
}
