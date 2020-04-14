package quest.util;

public class SHA256 {
    public static byte[] hash(String x) {
        byte[] result = {};

        try {
            java.security.MessageDigest d = null;
            d = java.security.MessageDigest.getInstance("SHA-256");
            d.reset();
            d.update(x.getBytes());
            result = d.digest();
        } catch (Exception e) {
            System.out.println("Exception: " + e);
        }

        return result;
    }
}
