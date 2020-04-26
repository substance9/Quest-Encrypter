package quest.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public class AES {

    private static SecretKeySpec secretKey;
    private static byte[] key = {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15};

    public static void setKey()
    {
        try {
            secretKey = new SecretKeySpec(key, "AES");
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

//    public static void setKey(String myKey)
//    {
//        MessageDigest sha = null;
//        try {
//            key = myKey.getBytes("UTF-8");
//            sha = MessageDigest.getInstance("SHA-1");
//            key = sha.digest(key);
//            System.out.println(key);
//            key = Arrays.copyOf(key, 16);
//            secretKey = new SecretKeySpec(key, "AES");
//        }
//        catch (NoSuchAlgorithmException e) {
//            e.printStackTrace();
//        }
//        catch (UnsupportedEncodingException e) {
//            e.printStackTrace();
//        }
//        catch (Exception e){
//            e.printStackTrace();
//        }
//    }

    public static String encrypt(String strToEncrypt)
    {
        try
        {
            setKey();
            byte[] iv = new byte[12];
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            GCMParameterSpec parameterSpec = new GCMParameterSpec(128, iv); //128 bit auth tag length
            cipher.init(Cipher.ENCRYPT_MODE, secretKey, parameterSpec);
            byte[] cipherText = cipher.doFinal(strToEncrypt.getBytes(StandardCharsets.US_ASCII));
//            ByteBuffer byteBuffer = ByteBuffer.allocate(4 + iv.length + cipherText.length);
            ByteBuffer byteBuffer = ByteBuffer.allocate(iv.length + cipherText.length);

        //    byteBuffer.putInt(iv.length);
            byteBuffer.put(iv);
            byteBuffer.put(cipherText);
            byte[] cipherMessage = byteBuffer.array();
            return Base64.getEncoder().encodeToString(cipherMessage);
        }
        catch (Exception e)
        {
            System.out.println("Error while encrypting: " + e.toString());
        }
        return null;
    }

    public static String decrypt(String strToDecrypt)
    {
        try
        {
            byte[] ciphertext = Base64.getDecoder().decode(strToDecrypt);
            ByteBuffer byteBuffer = ByteBuffer.wrap(ciphertext);
            byte[] iv = new byte[12];
            byteBuffer.get(iv);
            byte[] encrypted = new byte[byteBuffer.remaining()];
            byteBuffer.get(encrypted);

            setKey();
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            GCMParameterSpec parameterSpec = new GCMParameterSpec(128, iv); //128 bit auth tag length
            cipher.init(Cipher.DECRYPT_MODE, secretKey, parameterSpec);
            return new String(cipher.doFinal(encrypted));
        }
        catch (Exception e)
        {
            System.out.println("Error while decrypting: " + e.toString());
            e.printStackTrace();
        }
        return null;
    }

    public static String concatAndEncrypt(String ...strsToEncrypt){
        String concatSeperator = "||";
        String concatRawStr = String.join(concatSeperator, strsToEncrypt);

        return encrypt(concatRawStr);
    }

    public static void main(String[] args) throws IOException {
        String timeStr = "2018-12-25 23:59:53";
        String locStr = "3082-clwa-2130";
        DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long epoch = 0;
        try {

            Date date = sdf.parse(timeStr);
            epoch = date.getTime();
        }catch (Exception e){
            e.printStackTrace();
            return;
        }
        epoch = epoch / 1000;
        System.out.println("unencrypted: " + locStr+"||"+Long.toString(epoch)+"||"+"1");
        System.out.println("encrypted: " + encrypt(locStr+"||"+Long.toString(epoch)+"||"+"1"));

        System.out.println("direct encrypted: " + encrypt("3082-clwa-2110||1547751088||28"));

        String encStr = "AAAAAAAAAAAAAAAAfua1aqCvnr/X9QZdULeCsNrBXE8ZC7ZKOShbcqmHymUdANqoawG43+t0NKCjB7LaNeNmDnT1hEvR2XaRXxfo9A==";

        System.out.println("decrypt: " + decrypt(encStr));
    }
}