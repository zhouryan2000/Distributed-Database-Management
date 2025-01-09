package shared.messages;

public class KVMessageEntity implements KVMessage {
    private StatusType status;
    private String key;
    private String value;
    private static final char LINE_FEED = 10;
    private static final char RETURN = 13;
    private static final String DELIMITER = " ";

    public KVMessageEntity(StatusType status, String key, String value) {
        this.status = status;
        this.key = key;
        this.value = value;
    }

    public KVMessageEntity(byte[] bytes) {
        String msg = new String(rmvCtrChars(bytes));
        String[] arr = msg.split(DELIMITER, 3);

        this.key = null;
        this.value = null;
        this.status = StatusType.UNKNOWN;
        if (arr.length >= 1) {
            this.status = stringToStatusType(arr[0]);
        }
        if (arr.length >= 2) {
            this.key = arr[1];
        }
        if (arr.length >= 3) {
            this.value =arr[2];
        }
    }

    /**
     * Convert string to KVMessage.StatusType
     * @param status represented in String
     * @return
     */
    private StatusType stringToStatusType(String status) {
        status = status.toUpperCase();

        try {
            return StatusType.valueOf(status);
        } catch (Exception e) {
            return StatusType.UNKNOWN;
        }
    }

    private byte[] addCtrChars(byte[] bytes) {
        byte[] ctrBytes = new byte[]{RETURN, LINE_FEED};
        byte[] tmp = new byte[bytes.length + ctrBytes.length];

        System.arraycopy(bytes, 0, tmp, 0, bytes.length);
        System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);

        return tmp;
    }

    public static byte[] rmvCtrChars(byte[] bytes) {
        byte[] ctrBytes = new byte[]{RETURN, LINE_FEED};
        if (bytes[bytes.length - 1] == ctrBytes[1] && bytes[bytes.length - 2] == ctrBytes[0]) {
            byte[] tmp = new byte[bytes.length - ctrBytes.length];
            System.arraycopy(bytes, 0, tmp, 0, bytes.length - ctrBytes.length);
            return tmp;
        }
        if (bytes[bytes.length - 1] == ctrBytes[0]) {
            byte[] tmp = new byte[bytes.length - 1];
            System.arraycopy(bytes, 0, tmp, 0, bytes.length - 1);
            return tmp;
        }
        return bytes;
    }

    public StatusType getStatus() {
        return status;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public void setStatus(StatusType status) {
        this.status = status;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String toString() {
        String s = status.toString();
        if (key != null) s += DELIMITER + key;
        if (value != null) s += DELIMITER + value;
        return s;
    }

    public byte[] toByteArray() {
        String msg;
        if (value != null && key != null) {
            msg = this.toString();
        }
        else if (key != null) {
            msg = status.toString() + DELIMITER + key;
        } else {
            msg = status.toString();
        }
        return addCtrChars(msg.getBytes());
    }

    public String outputFormat() {
        return "Status: " + this.status + ", key: " + this.key +", value: " + this.value;
    }
}