package database;

public class KVPair {
    String key;
    String value;
    long startOffset;
    long endOffset;

    public KVPair(String key, String value, long startOffset, long endOffset) {
        this.key = key;
        this.value = value;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    public KVPair(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
