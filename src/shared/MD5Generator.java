package shared;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5Generator {
    public static BigInteger generateHash(String input) {
        String tableDelimiter = "@";
        try {
            if (input.contains(tableDelimiter)) {
                input = input.split(tableDelimiter)[0];
            }
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] messageDigest = md.digest(input.getBytes());
            BigInteger value = new BigInteger(1, messageDigest);
            return value;
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
