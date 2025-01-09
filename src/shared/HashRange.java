package shared;

import java.math.BigInteger;

public class HashRange {
    private BigInteger startIndex;
    private BigInteger endIndex;
    private static final BigInteger maximum = new BigInteger("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", 16);


    public HashRange() {
        this.startIndex = null;
        this.endIndex = null;
    }

    public HashRange(BigInteger startIndex, BigInteger endIndex) {
        this.startIndex = startIndex;
        this.endIndex = endIndex;
    }

    public HashRange(String range){
        String[] values = range.split(",");
        this.startIndex = new BigInteger(values[0], 16);
        this.endIndex = new BigInteger(values[1], 16);
    }

    public BigInteger getStartIndex() {
        return startIndex;
    }

    public void setStartIndex(BigInteger startIndex) {
        this.startIndex = startIndex;
    }

    public BigInteger getEndIndex() {
        return endIndex;
    }

    public void setEndIndex(BigInteger endIndex) {
        this.endIndex = endIndex;
    }

    public String toString(){
        return startIndex.toString(16) + "," + endIndex.toString(16);
    }

    public boolean isInRange(BigInteger hash){
        // not connect to ECS case
        if (this.startIndex == null && this.endIndex == null) {
            return true;
        }

        // startIndex > endIndex
        if (startIndex.compareTo(endIndex) == 1) {
            if (hash.compareTo(BigInteger.ZERO) >= 0 && hash.compareTo(endIndex) <= 0) {
                return true;
            }
            else if (hash.compareTo(startIndex) >= 0 && hash.compareTo(maximum) <= 0) {
                return true;
            }
            else {
                return false;
            }
        }

        if (hash.compareTo(endIndex) <= 0  && hash.compareTo(startIndex) >= 0) {
            return true;
        }
        else {
            return false;
        }

    }
}
