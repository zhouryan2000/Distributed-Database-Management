package shared;

import shared.Pair;
import org.apache.log4j.Logger;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import static shared.MD5Generator.generateHash;

public class Metadata {
    private static Logger logger = Logger.getLogger(Metadata.class);
    // map server to key range
    // each pair is followed by successor in order
    public List<Pair<String, HashRange>> server2rangeList = new Vector<Pair<String, HashRange>>(1);
    private static final BigInteger maximum = new BigInteger("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", 16);

    private String tableDelimiter = "@";

    public Metadata() {

    }

    public Metadata(String address, int port, HashRange range) {
        String key = address + ":" + String.valueOf(port);
        Pair<String, HashRange> new_pair = new Pair<>(key, range);
        server2rangeList.add(new_pair);
    }

    public Metadata(String value) {
        String[] tokens = value.split(";");

        for(int i = 0; i < tokens.length; i++) {
            String[] vals = tokens[i].split(",");
            BigInteger startIndex = new BigInteger(vals[0], 16);
            BigInteger endIndex = new BigInteger(vals[1], 16);
            this.addServer(vals[2], startIndex, endIndex);
        }
    }

    public boolean isEmpty(){
        return server2rangeList.isEmpty();
    }

    public String toString() {
        String s = "";

        for (int i = 0; i < server2rangeList.size(); i++) {
            s += server2rangeList.get(i).snd.getStartIndex().toString(16) + ",";
            s += server2rangeList.get(i).snd.getEndIndex().toString(16) + ",";
            s += server2rangeList.get(i).fst + ";";
        }

        return s;
    }

    public Pair<HashRange, String> addServer(String hostname, int port) {
        String serverAddress = hostname + ":" + port;
        BigInteger hashValue = generateHash(serverAddress);
        BigInteger nextStartIndex = hashValue.add(BigInteger.ONE);

        HashRange newRange;

        if (server2rangeList.isEmpty()) {
            newRange = new HashRange(nextStartIndex, hashValue);
            server2rangeList.add(new Pair<>(serverAddress, newRange));
            return new Pair<>(newRange, null);
        }
        else{
            for (int i = 0; i < server2rangeList.size(); i++) {
                if(server2rangeList.get(i).snd.isInRange(hashValue)) {
                    logger.debug(String.format("Metadata: new hash value %s is in server [%s]'s range (%s)",
                            hashValue.toString(16), server2rangeList.get(i).fst, server2rangeList.get(i).snd));

                    // from predecessor's start index to hashvalue
                    newRange = new HashRange(server2rangeList.get(i).snd.getStartIndex(), hashValue);
                    server2rangeList.add(i, new Pair<>(serverAddress, newRange));


                    server2rangeList.get(i+1).snd.setStartIndex(nextStartIndex);
                    return new Pair<>(newRange, server2rangeList.get(i+1).fst);
                }
            }
        }
        return null;
    }

    public void addServer(String address, BigInteger startIndex, BigInteger endIndex){
        HashRange range = new HashRange(startIndex, endIndex);
        server2rangeList.add(new Pair<>(address, range));
    }

    // remove successor name &
    public Pair<String, HashRange> removeServer(String hostname, int port) {
        String serverAddress = hostname + ":" + port;
        BigInteger hashValue = generateHash(serverAddress);

        if (server2rangeList.size() == 1 && server2rangeList.get(0).fst.compareTo(serverAddress) == 0){
            server2rangeList.remove(0);
            return null;
        }

        for (int i = 0; i < server2rangeList.size(); i++) {
            if(server2rangeList.get(i).fst.compareTo(serverAddress) == 0) {
                Pair<String, HashRange> toBeRemoved = server2rangeList.get(i);

                //removing last one
                if (i == server2rangeList.size() - 1) {
                    server2rangeList.get(0).snd.setStartIndex(toBeRemoved.snd.getStartIndex());
                    server2rangeList.remove(i);
                    return server2rangeList.get(0);
                }
                else {
                    server2rangeList.get(i + 1).snd.setStartIndex(toBeRemoved.snd.getStartIndex());
                    server2rangeList.remove(i);
                    return server2rangeList.get(i);
                }
            }
        }

        return null;
    }

    // return "address:port"
    public String findResponsibleServer(String key){

        if (key.contains(tableDelimiter)) {
            key = key.split(tableDelimiter)[0];
        }

        BigInteger hashValue = generateHash(key);
        for (int i = 0; i < server2rangeList.size(); i++) {
            if(server2rangeList.get(i).snd.isInRange(hashValue))
                return server2rangeList.get(i).fst;
        }
        return null;
    }

    public HashRange getHashRange(String address){
        for (int i = 0; i < server2rangeList.size(); i++) {
            if(server2rangeList.get(i).fst.compareTo(address) == 0)
                return server2rangeList.get(i).snd;
        }
        return null;
    }

    public int getSize() {
        return this.server2rangeList.size();
    }

    public ArrayList<String> getAllServer() {
        ArrayList<String> result = new ArrayList<>();

        for (Pair<String, HashRange> pair: server2rangeList) {
            result.add(pair.fst);
        }

        return result;
    }

    /*M3*/
    public Pair<String, HashRange> getSuccessorAt(String address, int n) {

        for (int i = 0; i < server2rangeList.size(); i++) {
            if(server2rangeList.get(i).fst.compareTo(address) == 0) {
                int size = server2rangeList.size();
                int j = (i + n + size) % size;
                return server2rangeList.get(j);
            }
        }

        return null;
    }

    public String toString(boolean includeReplica) {
        if (!includeReplica) {
            return this.toString();
        }

        String s = "";
        int size = server2rangeList.size();
        int j;
        int num;

        if (size >= 3) num = 2;
        else if (size == 2) num = 1;
        else num = 0;

        for (int i = 0; i < size; i++) {
            j = (i + size - num) % size;
            s += server2rangeList.get(j).snd.getStartIndex().toString(16) + ",";
            s += server2rangeList.get(i).snd.getEndIndex().toString(16) + ",";
            s += server2rangeList.get(i).fst + ";";
        }

        return s;
    }

    public int getMetadataSize() {
        return this.server2rangeList.size();
    }

}
