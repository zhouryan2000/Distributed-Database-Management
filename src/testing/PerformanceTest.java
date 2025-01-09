package testing;

import app_kvServer.KVServer;
import client.KVStore;
import junit.framework.TestCase;
import org.junit.Test;

import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Random;

public class PerformanceTest extends TestCase {
    private KVServer kvServer;
    private KVStore kvClient;
    private String value;
    private ArrayList<String> keys = new ArrayList<>();
    Random random = new Random(5507);

    public void setUp() {
        kvClient = new KVStore("localhost", 50000);

        byte[] data_bytes;

        data_bytes = new byte[20];

        random.nextBytes(data_bytes);
        value = new String(data_bytes, StandardCharsets.UTF_8);

        for (int i = 1; i <= 100; i++) {
            keys.add("key" + i);
        }

        try {
            kvClient.connect();
        } catch (Exception e) {
            System.err.println(e);
        }
    }

    public void tearDown() {
        kvClient.disconnect();
    }

    @Test
    public void testPerf_100PUT() {
        int iteration = 50000;
        long startTime, endTime, interval;
        double latency;
        Random r = new Random(66);
        int nget = 0, nput = 0;

        startTime = System.currentTimeMillis();
        for (int i = 0; i < iteration; i++) {
            try {
                if (r.nextDouble() <= 1) {
                    nput += 1;
                    kvClient.put(keys.get(i % 100), value);
                } else {
                    nget += 1;
                    kvClient.get(keys.get(i % 100));
                }
            } catch (Exception e) {
                System.out.println(e);
                System.out.println(keys.get(i % 100));
            }
        }

        endTime = System.currentTimeMillis();
        interval = endTime - startTime;

        latency = 1000 * interval / (iteration);

        System.out.println("100% PUT Latency: " + latency + " ms");
        System.out.println("number of put operations: " + nput);
        System.out.println("number of get operations: " + nget);
    }

    @Test
    public void testPerf_80GET_20PUT() {
        int iteration = 50000;
        long startTime, endTime, interval;
        double latency;
        Random r = new Random(66);
        int nget = 0, nput = 0;

        startTime = System.currentTimeMillis();
        for (int i = 0; i < iteration; i++) {
            try {
                if (r.nextDouble() <= 0.2) {
                    nput += 1;
                    kvClient.put(keys.get(i % 100), value);
                } else {
                    nget += 1;
                    kvClient.get(keys.get(i % 100));
                }
            } catch (Exception e) {
                System.out.println(e);
                System.out.println(keys.get(i % 100));
            }
        }

        endTime = System.currentTimeMillis();
        interval = endTime - startTime;

        latency = 1000 * interval / (iteration);

        System.out.println("80% GET 20% PUT Latency: " + latency + " ms");
        System.out.println("number of put operations: " + nput);
        System.out.println("number of get operations: " + nget);
    }

    @Test
    public void testPerf_50GET_50PUT() {
        int iteration = 50000;
        long startTime, endTime, interval;
        double latency;
        Random r = new Random(66);
        int nget = 0, nput = 0;

        startTime = System.currentTimeMillis();
        for (int i = 0; i < iteration; i++) {
            try {
                if (r.nextDouble() <= 0.5) {
                    nput += 1;
                    kvClient.put(keys.get(i % 100), value);
                } else {
                    nget += 1;
                    kvClient.get(keys.get(i % 100));
                }
            } catch (Exception e) {
                System.out.println(e);
                System.out.println(keys.get(i % 100));
            }
        }

        endTime = System.currentTimeMillis();
        interval = endTime - startTime;

        latency = 1000 * interval / (iteration);

        System.out.println("50% GET 50% PUT Latency: " + latency + " ms");
        System.out.println("number of put operations: " + nput);
        System.out.println("number of get operations: " + nget);
    }

    @Test
    public void testPerf_20GET_80PUT() {
        int iteration = 50000;
        long startTime, endTime, interval;
        double latency;
        Random r = new Random(66);
        int nget = 0, nput = 0;

        startTime = System.currentTimeMillis();
        for (int i = 0; i < iteration; i++) {
            try {
                if (r.nextDouble() <= 0.8) {
                    nput += 1;
                    kvClient.put(keys.get(i % 100), value);
                } else {
                    nget += 1;
                    kvClient.get(keys.get(i % 100));
                }
            } catch (Exception e) {
                System.out.println(e);
                System.out.println(keys.get(i % 100));
            }
        }

        endTime = System.currentTimeMillis();
        interval = endTime - startTime;

        latency = 1000 * interval / (iteration);

        System.out.println("20% GET 80% PUT Latency: " + latency + " ms");
        System.out.println("number of put operations: " + nput);
        System.out.println("number of get operations: " + nget);
    }

}
