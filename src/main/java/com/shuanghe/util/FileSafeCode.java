package com.shuanghe.util;

import org.apache.commons.codec.digest.DigestUtils;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.zip.CRC32;

/**
 * Description:计算字符串和文件的MD5与SHA1
 * <p>
 * [Java计算字符串和文件的MD5与SHA1 - CSDN博客](https://blog.csdn.net/delavior/article/details/41804133)
 * <p>
 * <p>
 * Date: 2018/09/27
 * Time: 15:09
 *
 * @author Shuanghe Yu
 */
public class FileSafeCode {
    @SuppressWarnings("unused")
    public static final String MD5 = "MD5";
    @SuppressWarnings("unused")
    public static final String SHA1 = "SHA-1";

    /**
     * 计算大文件 md5获取getMD5(); SHA1获取getSha1() CRC32获取 getCRC32()
     */
    protected static char[] hexDigits = {'0', '1', '2', '3', '4', '5', '6',
            '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    public static MessageDigest messagedigest = null;

    /**
     * 对一个文件获取md5值
     *
     * @return md5串
     * @throws NoSuchAlgorithmException
     */
    public static String getMD5(File file) throws IOException,
            NoSuchAlgorithmException {

        messagedigest = MessageDigest.getInstance("MD5");
        FileInputStream in = new FileInputStream(file);
        FileChannel ch = in.getChannel();
        MappedByteBuffer byteBuffer = ch.map(FileChannel.MapMode.READ_ONLY, 0,
                file.length());
        messagedigest.update(byteBuffer);
        return bufferToHex(messagedigest.digest());
    }

    /**
     * @param target 字符串 求一个字符串的md5值
     * @return md5 value
     */
    public static String StringMD5(String target) {
        return DigestUtils.md5Hex(target);
    }

    /***
     * 计算SHA1码
     *
     * @return String 适用于上G大的文件
     * @throws NoSuchAlgorithmException
     * */
    public static String getSha1(File file) throws OutOfMemoryError,
            IOException, NoSuchAlgorithmException {
        messagedigest = MessageDigest.getInstance("SHA-1");
        FileInputStream in = new FileInputStream(file);
        FileChannel ch = in.getChannel();
        MappedByteBuffer byteBuffer = ch.map(FileChannel.MapMode.READ_ONLY, 0,
                file.length());
        messagedigest.update(byteBuffer);
        return bufferToHex(messagedigest.digest());
    }

    /**
     * 获取文件CRC32码
     *
     * @return String
     */
    public static String getCRC32(File file) {
        CRC32 crc32 = new CRC32();
        // MessageDigest.get
        FileInputStream fileInputStream = null;
        try {
            fileInputStream = new FileInputStream(file);
            byte[] buffer = new byte[8192];
            int length;
            while ((length = fileInputStream.read(buffer)) != -1) {
                crc32.update(buffer, 0, length);
            }
            return crc32.getValue() + "";
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        } finally {
            try {
                if (fileInputStream != null) {
                    fileInputStream.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static String getMD5String(String s) {
        return getMD5String(s.getBytes());
    }

    public static String getMD5String(byte[] bytes) {
        messagedigest.update(bytes);
        return bufferToHex(messagedigest.digest());
    }

    /**
     * @return String
     * @Description 计算二进制数据
     */
    private static String bufferToHex(byte[] bytes) {
        return bufferToHex(bytes, 0, bytes.length);
    }

    private static String bufferToHex(byte[] bytes, int m, int n) {
        StringBuffer stringbuffer = new StringBuffer(2 * n);
        int k = m + n;
        for (int l = m; l < k; l++) {
            appendHexPair(bytes[l], stringbuffer);
        }
        return stringbuffer.toString();
    }

    private static void appendHexPair(byte bt, StringBuffer stringbuffer) {
        char c0 = hexDigits[(bt & 0xf0) >> 4];
        char c1 = hexDigits[bt & 0xf];
        stringbuffer.append(c0);
        stringbuffer.append(c1);
    }

    public static boolean checkPassword(String password, String md5PwdStr) {
        String s = getMD5String(password);
        return s.equals(md5PwdStr);
    }

    /**
     * @param str
     * @param type md5 or sha1
     * @return
     * @throws UnsupportedEncodingException
     * @desc 计算字符串的md5 or sha1
     */
    public static String md5OrSha1OfString(String str, String type)
            throws UnsupportedEncodingException {
        try {
            MessageDigest md = MessageDigest.getInstance(type);
            byte[] inputByteArr = str.getBytes("UTF-8");
            md.update(inputByteArr);
            byte[] rsByteArr = md.digest();
            return byteArrayToHex(rsByteArr);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * @param inputFile
     * @param type      md5    or sha1
     * @return
     * @throws IOException
     * @desc 计算文件的md5 or sha1
     */
    @SuppressWarnings("finally")
    public static String md5OrSha1OfFile(String inputFile, String type)
            throws IOException {
        String result = null;

        int bufferSize = 256 * 1024;
        FileInputStream fis = null;
        DigestInputStream dis = null;

        try {
            MessageDigest md = MessageDigest.getInstance(type);
            fis = new FileInputStream(inputFile);
            dis = new DigestInputStream(fis, md);
            byte[] buffer = new byte[bufferSize];
            while (dis.read(buffer) > 0) {
                md = dis.getMessageDigest();
                byte[] arr = md.digest();
                result = byteArrayToHex(arr);
            }
            return result;
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } finally {
            try {
                dis.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            dis = null;
            try {
                fis.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            fis = null;
        }
        return result;
    }

    /**
     * @param arr
     * @return
     * @desc 将生成的字节数组转化为十六进制字符串
     */
    public static String byteArrayToHex(byte[] arr) {
        String hexStr = "0123456789ABCDEF";
        StringBuilder rslt = new StringBuilder();
        for (int i = 0; i < arr.length; i++) {
            rslt.append(String.valueOf(hexStr.charAt((arr[i] & 0xf0) >> 4)));
            rslt.append(String.valueOf(hexStr.charAt(arr[i] & 0x0f)));
        }
        return rslt.toString();
    }
}