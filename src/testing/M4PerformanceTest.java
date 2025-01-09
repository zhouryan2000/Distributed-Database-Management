package testing;

import app_kvECS.ECSClient;
import app_kvServer.IKVServer;
import app_kvServer.KVServer;
import client.KVStore;
import ecs.ECSNode;
import junit.framework.TestCase;
import org.apache.log4j.BasicConfigurator;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class M4PerformanceTest extends TestCase {
    public static ECSClient ecsClient;
    private static Thread ecsThread;
    private ArrayList<Thread> serverThreads = new ArrayList<>(100);
    static final int numServer = 10;
    static final int numClient = 5;
    int numGet = 500;
    int numPut = 500;
    final int StartPort = 6000;
    long difference = 0;

    private HashMap<Integer, KVServer> servers = new HashMap<>();
    private HashMap<Integer, KVStore> clients = new HashMap<>();

    private final String ENRON_SET = "/Users/zhouxingjian/Documents/UofT/maildir/allen-p/all_documents";
    private File[] dirList;

    int ECSport = 7000;

    ExecutorService taskExecutor;


    public void setUpECS() {
        // initialize an ECSClient and run it on a new thread
        System.out.println("initialize ECS");

        ecsThread = new Thread(new Runnable() {
            @Override
            public void run() {
                ecsClient = new ECSClient("localhost", ECSport);
                ecsClient.run();
            }
        });
        ecsThread.start();
    }

    public void createServer(int port, final int index) {

        System.out.println("Creating server...");
        try{
            servers.put(index, new KVServer(port, 10, "FIFO", "disk", "test_data_set_" + port,
                    "localhost", "localhost:" + ECSport));
            //kvServer.run();
        } catch(Exception e) {
            ;
        }

        serverThreads.add(index, new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    servers.get(index).run();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }));

        serverThreads.get(index).start();

        while (servers.get(index).getServerStatus() != IKVServer.ServerStatus.ACTIVE){
            try {
                Thread.sleep(0);
            } catch (InterruptedException e) {
                System.out.println("Sleep interrupted");
            }
        }

        System.out.println("Server at [" + port + "] is active");
    }

    class ShutdownPrint extends Thread {
        public void run() {
            System.out.println(String.format("Total runtime for %d servers and %d clients with %d put operation and" +
                    " %d get operation is %d ms", numServer, numClient, numPut, numGet, difference));
            System.out.println(((float) 1000 / difference) * 1000);
        }
    }

    private class ClientThread implements Runnable {
        int clientID;
        int port;
        String address;
        int getNumber;
        int putNumber;

        public ClientThread(String address, int port, int id, int getNumber, int putNumher) {
            this.address = address;
            this.port = port;
            this.clientID = id;
            this.getNumber = getNumber;
            this.putNumber = putNumher;

            System.out.println("numbers: " + getNumber + " " + putNumher);
        }

        @Override
        public void run() {
            clients.put(clientID, new KVStore("localhost", port));

            try {
                KVStore client = clients.get(clientID);
                client.connect();

                int i = 0;

                File dir = new File(ENRON_SET);

                for (File temp : dir.listFiles()) {
                    String key = temp.getPath();
                    key = key.substring(key.length() - 10, key.length() - 1);
                    String value = new String(Files.readAllBytes(temp.toPath())).substring(0, 30);

                    //                    System.out.println(key + " ::: " + value);
                    client.put(key, value);

                    i += 1;
                    //                    System.out.println(i);

                    if (i == putNumber) break;
                }

                System.out.println(i);

                i = 0;

                for (File temp : dir.listFiles()) {
                    String key = temp.getPath();
                    key = key.substring(key.length() - 10, key.length() - 1);

                    //                    System.out.println(key + " ::: " + value);
                    client.get(key);

                    i += 1;
                    //                    System.out.println(i);

                    if (i == getNumber) break;
                }
            } catch(Exception e) {

            }
        }
    }

    @Test
    public void testPerformance() {
        BasicConfigurator.configure();
        taskExecutor = Executors.newFixedThreadPool(numClient);
        Runtime current = Runtime.getRuntime();
        current.addShutdownHook(new M4PerformanceTest.ShutdownPrint());

        setUpECS();

        for (int j = 0; j < numServer; j++) {
            createServer(StartPort + j, j);
        }

        final long startTime = System.nanoTime();

        try {

            for (int clientID = 0; clientID < numClient; clientID++) {
                int port = clientID % numServer + StartPort;

                taskExecutor.execute(new ClientThread("localhost", port, clientID,
                        numGet / numClient, numPut / numClient ));
            }
        } catch (Exception e) {

        }

        taskExecutor.shutdown(); //close the executor pool and wait for termination
        try {
            taskExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            System.out.println("Failed executing all threads");
        }

        final long endTime = System.nanoTime();

        difference = (endTime - startTime) / 1000000;
    }

//    @Test
//    public void testScaling() {
//        setUpECS();
//
//        long startTime = System.nanoTime();
//
//        for (int j = 0; j < numServer; j++) {
//            createServer(StartPort + j, j);
//            if (j == 0 || j == 4 || j == 9 || j == 19 || j == 49) {
//                long endTime = System.nanoTime();
//                System.out.println("cost time: " + ((endTime - startTime) / 1000000));
//            }
//        }
//
//        startTime = System.nanoTime();
//
//        for (int j = 0; j < 20; j++) {
//            try {
//                ecsClient.removeFailedNode((ECSNode) ecsClient.getNodeByKey("localhost:" + (StartPort + j)));
//            } catch (Exception e) {
//
//            }
//            while (servers.get(j).getServerStatus() != IKVServer.ServerStatus.STOP) {
//
//            };
//            if (j == 0 || j == 4 || j == 9 || j == 19 || j == 49) {
//                long endTime = System.nanoTime();
//                System.out.println("cost time: " + ((endTime - startTime) / 1000000));
//            }
//        }
//    }
}