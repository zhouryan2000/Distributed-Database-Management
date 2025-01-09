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

public class M4AdditionalTest extends TestCase {
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
    // Test 1:  ECS connection
    public void testECSConnection() {
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
    }


    @Test
    // Test 2: Test When have 2 servers, replica of table info works successfully
    public void testTableOneReplica() {
        setUpECS(6001);

        KVServer server1 = createServer("localhost", 5101, "test_data_set_1", 6001);
        name2Servers.put("localhost:5101", server1);
        KVServer server2 = createServer("localhost", 5102, "test_data_set_2", 6001);
        name2Servers.put("localhost:5102", server2);
        KVStore kvClient1 = new KVStore("localhost", 5101);
        KVStore kvClient2 = new KVStore("localhost", 5102);

        KVMessage response1 = null;
        KVMessage response2 = null;
        KVMessage response3 = null;
        KVMessage response4 = null;
        Exception ex = null;

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        try {
            kvClient1.connect();
            kvClient2.connect();
            response1 = kvClient1.put("a", "b");
            response2 = kvClient2.put("apple", "banana");
            response3 = kvClient2.get("a");
            response4 = kvClient1.get("a");
            kvClient1.disconnect();
            kvClient2.disconnect();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null);
    }


    @Test
    // Test 3: Test When have 3 servers, replica of table info works successfully
    public void testThreeTableServer() {
        setUpECS(6002);

        KVServer server3 = createServer("localhost", 5103, "test_data_set_3", 6002);
        name2Servers.put("localhost:5103", server3);
        KVServer server4 = createServer("localhost", 5104, "test_data_set_4", 6002);
        name2Servers.put("localhost:5104", server4);
        KVServer server5 = createServer("localhost", 5105, "test_data_set_5", 6002);
        name2Servers.put("localhost:5105", server5);
        KVStore kvClient3 = new KVStore("localhost", 5103);
        KVStore kvClient4 = new KVStore("localhost", 5104);
        KVStore kvClient5 = new KVStore("localhost", 5105);

        Exception ex = null;

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        try {
            kvClient3.connect();
            kvClient4.connect();
            kvClient5.connect();
            // moon is in server4's range
            kvClient3.put("student@columns", "a,b");
            // hello is in server5's range
            kvClient5.put("student@id", "0");
            kvClient4.get("student@id");
            kvClient4.get("student@columns");
            kvClient5.get("student@1");
            kvClient5.get("student@2");
            kvClient3.disconnect();
            kvClient4.disconnect();
            kvClient5.disconnect();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null);
    }


    @Test
    // Test 4: Test whether the select query works correctly
    public void testSelectQuery() {
        // moon is now in 6
        KVServer server6 = createServer("localhost", 5106, "test_data_set_6", 6002);
        name2Servers.put("localhost:5106", server6);
        KVStore kvClient6 = new KVStore("localhost", 5106);

        Exception ex = null;

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        try {
            kvClient6.connect();
            kvClient6.get("student@id");
            // server6 can get hello because now it is replica of server5
            kvClient6.selectQuery("student", "a", "b > 0");
            kvClient6.selectQuery("student", "a", "b = 0");
            kvClient6.selectQuery("student", "a", "b != 0");
            kvClient6.selectQuery("student", "a", "b >= 0");
            kvClient6.selectQuery("student", "a", "b <= 0");
            kvClient6.selectQuery("student", "a", "b < 0");
            kvClient6.disconnect();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null);
    }


    @Test
    // Test 5: Test whether update query works correctly, and test whether both coordinator and
    // replica servers update their disk files
    public void testUpdateQuery() {
        setUpECS(6003);

        // 8 -> 7 -> 9 -> 10

        KVServer server7 = createServer("localhost", 5107, "test_data_set_7", 6003);
        name2Servers.put("localhost:5107", server7);
        KVServer server8 = createServer("localhost", 5108, "test_data_set_8", 6003);
        name2Servers.put("localhost:5108", server8);
        KVServer server9 = createServer("localhost", 5109, "test_data_set_9", 6003);
        name2Servers.put("localhost:5109", server9);
        KVServer server10 = createServer("localhost", 5110, "test_data_set_10", 6003);
        name2Servers.put("localhost:5110", server10);
        KVStore kvClient7 = new KVStore("localhost", 5107);
        KVStore kvClient8 = new KVStore("localhost", 5108);
        KVStore kvClient9 = new KVStore("localhost", 5109);
        KVStore kvClient10 = new KVStore("localhost", 5110);

        Exception ex1 = null;
        Exception ex2 = null;

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        try {
            kvClient7.connect();
            kvClient8.connect();
            kvClient9.connect();
            kvClient10.connect();
            // main is in server 7's range, 2 replicas are 9, 10
            kvClient7.CreateTable("student", "a,b");
            kvClient7.put("student@1", "1,2");
            kvClient7.get("student@1");
            kvClient9.get("student@1");
            kvClient10.get("student@1");
            kvClient7.updateQuery("student", "a = 1", "b > 0");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            ex1 = e;
        }

        server9.runShutDownHook();

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        try {
            kvClient7.get("man");
            // now server 8 also has man
            kvClient8.get("man");
            kvClient10.get("man");
            kvClient7.disconnect();
            kvClient8.disconnect();
            kvClient9.disconnect();
            kvClient10.disconnect();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            ex2 = e;
        }

        assertTrue(ex1 == null && ex2 == null);
    }


    @Test
    // Test 6: Test create multiple tables
    public void testCreateTable() {
        setUpECS(6004);

        // server 12 -> server 11 -> server 13 -> server 14

        KVServer server11 = createServer("localhost", 5111, "test_data_set_11", 6004);
        name2Servers.put("localhost:5111", server11);
        KVServer server12 = createServer("localhost", 5112, "test_data_set_12", 6004);
        name2Servers.put("localhost:5112", server12);
        KVServer server13 = createServer("localhost", 5113, "test_data_set_13", 6004);
        name2Servers.put("localhost:5113", server13);
        KVServer server14 = createServer("localhost", 5114, "test_data_set_14", 6004);
        name2Servers.put("localhost:5114", server14);

        Exception ex = null;

        KVStore kvClient11 = new KVStore("localhost", 5111);
        KVStore kvClient12 = new KVStore("localhost", 5112);
        KVStore kvClient13 = new KVStore("localhost", 5113);
        KVStore kvClient14 = new KVStore("localhost", 5114);

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        try {
            kvClient11.connect();
            kvClient12.connect();
            kvClient13.connect();
            kvClient14.connect();
            // main is in server 7's range, 2 replicas are 9, 10
            kvClient11.CreateTable("student", "a,b");
            kvClient12.CreateTable("teacher", "c,d");
            kvClient13.CreateTable("people", "name,mark");
            kvClient12.put("student@1", "1,2");
            kvClient13.get("student@columns");
            kvClient14.get("teacher@columns");
            kvClient11.get("student@columns");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null);
    }


    @Test
    // Test 7: Test one client create table and multiple client insert values
    public void testInsertValues() {
        setUpECS(6005);

        KVServer server15 = createServer("localhost", 5115, "test_data_set_15", 6005);
        name2Servers.put("localhost:5115", server15);
        KVServer server16 = createServer("localhost", 5116, "test_data_set_16", 6005);
        name2Servers.put("localhost:5116", server16);
        KVServer server17 = createServer("localhost", 5117, "test_data_set_17", 6005);
        name2Servers.put("localhost:5117", server17);
        KVServer server18 = createServer("localhost", 5118, "test_data_set_18", 6005);
        name2Servers.put("localhost:5118", server18);

        KVStore kvClient15 = new KVStore("localhost", 5115);
        KVStore kvClient16 = new KVStore("localhost", 5116);
        KVStore kvClient17 = new KVStore("localhost", 5117);
        KVStore kvClient18 = new KVStore("localhost", 5118);

        Exception ex1 = null;

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        HashRange range15 = server15.getResponsibleRange();
        HashRange range16 = server16.getResponsibleRange();
        HashRange range17 = server17.getResponsibleRange();

        assertTrue(range16.getStartIndex().equals(range15.getEndIndex().add(BigInteger.valueOf(1)))
                && range17.getStartIndex().equals(range16.getEndIndex().add(BigInteger.valueOf(1)))
        );

        try {
            kvClient15.connect();
            kvClient16.connect();
            kvClient17.connect();
            kvClient18.connect();
            kvClient16.CreateTable("student", "a,b");
            kvClient17.insertTableValues("student", "1,2");
            kvClient18.insertTableValues("student", "3,4");
            kvClient17.insertTableValues("student", "5,6");
            kvClient18.insertTableValues("student", "7,8");
            kvClient17.insertTableValues("student", "11,22");
            kvClient18.insertTableValues("student", "33,44");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            ex1 = e;
        }

        assertTrue(ex1 == null);
    }


    @Test
    // Test 8: Test persistency
    public void testStoragePersistency() {
        setUpECS(6006);

        KVServer server19 = createServer("localhost", 5119, "test_data_set_19", 6006);
        name2Servers.put("localhost:5119", server19);
        KVServer server20 = createServer("localhost", 5120, "test_data_set_20", 6006);
        name2Servers.put("localhost:5120", server20);
        KVServer server21 = createServer("localhost", 5121, "test_data_set_21", 6006);
        name2Servers.put("localhost:5121", server21);
        KVServer server22 = createServer("localhost", 5122, "test_data_set_22", 6006);
        name2Servers.put("localhost:5122", server22);

        KVStore kvClient19 = new KVStore("localhost", 5119);
        KVStore kvClient20 = new KVStore("localhost", 5120);
        KVStore kvClient21 = new KVStore("localhost", 5121);
        KVStore kvClient22 = new KVStore("localhost", 5122);

        KVMessage response1 = null;
        KVMessage response2 = null;
        Exception ex = null;

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        try {
            kvClient19.connect();
            kvClient20.connect();
            kvClient21.connect();
            kvClient22.connect();
            kvClient19.put("yes", "no");
            kvClient20.put("ilove", "thisworld");
            kvClient21.put("math", "problem");
            kvClient22.put("computer", "science");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {}

        server19.runShutDownHook();
        server20.runShutDownHook();
        server21.runShutDownHook();

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        try {
            kvClient19.get("yes");
            kvClient20.get("ilove");
            kvClient21.get("math");
            kvClient22.get("computer");
            kvClient19.disconnect();
            kvClient20.disconnect();
            kvClient21.disconnect();
            kvClient22.disconnect();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null);
    }


    @Test
    // Test 9: Test all kinds of Queries and functionality
    public void testQueries() {
        setUpECS(6007);

        KVServer server23 = createServer("localhost", 5123, "test_data_set_23", 6007);
        name2Servers.put("localhost:5123", server23);
        KVServer server24 = createServer("localhost", 5124, "test_data_set_24", 6007);
        name2Servers.put("localhost:5124", server24);
        KVServer server25 = createServer("localhost", 5125, "test_data_set_25", 6007);
        name2Servers.put("localhost:5125", server25);
        KVServer server26 = createServer("localhost", 5126, "test_data_set_26", 6007);
        name2Servers.put("localhost:5126", server26);

        KVStore kvClient23 = new KVStore("localhost", 5123);
        KVStore kvClient24 = new KVStore("localhost", 5124);
        KVStore kvClient25 = new KVStore("localhost", 5125);
        KVStore kvClient26 = new KVStore("localhost", 5126);

        Exception ex1 = null;
        Exception ex2 = null;

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        try {
            kvClient23.connect();
            kvClient24.connect();
            kvClient25.connect();
            kvClient26.connect();
            kvClient23.CreateTable("teacher", "a,b");
            kvClient25.insertTableValues("teacher", "111,222");
            kvClient25.insertTableValues("teacher", "132,227");
            kvClient26.deleteQuery("teacher", "a = 111");
            kvClient25.updateQuery("teacher", "b=419", "a = 132");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            ex1 = e;
        }

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        assertTrue(ex1 == null && ex2 == null);
    }


    @Test
    // Test 10: Test all kinds of Queries and functionality and whether replica is updated
    public void testOperationsAndReplicaUpdate() {
        setUpECS(6008);

        KVServer server28 = createServer("localhost", 5128, "test_data_set_24", 6008);
        name2Servers.put("localhost:5128", server28);
        KVServer server29 = createServer("localhost", 5129, "test_data_set_25", 6008);
        name2Servers.put("localhost:5129", server29);
        KVServer server30 = createServer("localhost", 5130, "test_data_set_26", 6008);
        name2Servers.put("localhost:5130", server30);

        KVStore kvClient24 = new KVStore("localhost", 5128);
        KVStore kvClient25 = new KVStore("localhost", 5129);
        KVStore kvClient26 = new KVStore("localhost", 5130);

        Exception ex1 = null;
        Exception ex2 = null;

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        try {
            kvClient24.connect();
            kvClient25.connect();
            kvClient26.connect();
            kvClient24.CreateTable("teacher", "a,b");
            kvClient25.insertTableValues("teacher", "111,222");
            kvClient25.insertTableValues("teacher", "132,227");
            kvClient26.deleteQuery("teacher", "a = 111");
            kvClient25.updateQuery("teacher", "b=419", "a = 132");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            ex1 = e;
        }

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        assertTrue(ex1 == null && ex2 == null);
    }

}
