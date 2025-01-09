package database;

public interface IDatabase {
    boolean put(String key, String value) throws Exception;
    String get(String key) throws Exception;
    boolean delete(String key) throws Exception;
    void eraseDisk();
}
