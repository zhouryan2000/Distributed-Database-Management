package database;

import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class DatabaseManager implements IDatabase{
    private static Logger logger = Logger.getLogger(DatabaseManager.class);
    private String databaseDir;
    private String databaseName;
    private Map<String, String> database;

    private File diskFile;
    private String className="DatabaseManager: ";

    public DatabaseManager(String databaseDir, String databaseName) {
        this.databaseDir = databaseDir;
        this.databaseName = databaseName;
        openDiskFile();
        this.database = new ConcurrentHashMap<>(loadHashMap());
    }

    @Override
    public synchronized boolean put(String key, String value) throws Exception {
        if (this.diskFile == null) {
            logger.error(className + "Put - No disk file opened");
            return false;
        }
        try {
            this.database.put(key, value);
            logger.info(className + String.format("PUT - Successfully insert key: %s and value: %s into database", key, value));
            saveDatabase();
            return true;
        } catch (Exception e) {
            logger.error(className + String.format("PUT - Cannot insert key: %s and value: %s into database", key, value));
            return false;
        }
    }

    @Override
    public synchronized String get(String key) throws Exception {
        try{
            String value = this.database.get(key);
            if (value == null){
                logger.warn(className + String.format("GET - Key:%s not found", key));
            }
            else {
                logger.info(className + String.format("GET - Successfully get key: %s and value: %s", key, value));
            }
            return value;
        } catch (Exception e){
            logger.error(className + "GET - Error when trying to get key: "+ key);
            return null;
        }
    }

    @Override
    public synchronized boolean delete(String key) {
        try{
            String value = this.database.remove(key);
            if (value == null){
                logger.error(className + String.format("Delete - Key:%s not found", key));
                return false;
            }
            saveDatabase();
            logger.info(className + String.format("Delete - Successfully delete key: %s and value: %s", key, value));
            return true;
        } catch (Exception e){
            logger.error(className + "Error when trying to delete key: "+ key);
            return false;
        }
    }

    @Override
    public synchronized void eraseDisk() {
        try {
            this.database.clear();
            saveDatabase();
        } catch (Exception e) {
            logger.error(className + "Cannot erase database");
        }
    }

    private synchronized void openDiskFile() {
        logger.info(className + "Open disk file");
        File dir = new File(this.databaseDir);
        if (!dir.exists()) {
            dir.mkdir();
            logger.error(className + "Directory for database files not exist");
            return;
        }
        this.diskFile = new File(this.databaseDir + "/" + this.databaseName);
        try {
            if (this.diskFile.createNewFile()) {
                logger.info(className + "Disk file is created successfully");
            } else {
                logger.info(className + "Disk file already exists");
            }
        } catch (IOException e) {
            logger.error(className + "Open disk file failed", e);
        }
    }

    private synchronized Map<String, String>loadHashMap() {
        Map<String, String> hashMap = new HashMap<>();
        Properties properties = new Properties();
        try{
            properties.load(new FileInputStream(this.databaseDir + '/' + this.databaseName));
            for (String key : properties.stringPropertyNames()) {
                hashMap.put(key, properties.get(key).toString());
            }
        }catch (IOException error){
            logger.error(className + "Load hashmap from file failed");
        }

        return hashMap;
    }

    private synchronized void saveDatabase() {
        Properties properties = new Properties();

        properties.putAll(this.database);
        try{
            properties.store(new FileOutputStream(this.databaseDir + "/" + this.databaseName), null);
        }catch (IOException e){
            logger.error(className + "Cannot saving database to file");
        }
    }

    /* M2 */
    public synchronized Set<String> getALlKeys() {
        return database.keySet();
    }

}