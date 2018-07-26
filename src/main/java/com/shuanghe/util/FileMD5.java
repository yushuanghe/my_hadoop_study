package com.shuanghe.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * Description:计算文件md5值
 * <p>
 * Date: 2018/07/26
 * Time: 20:12
 *
 * @author yushuanghe
 */
public class FileMD5 {

    //public static void main(String[] args) {
    //    String path = "C:\\Users\\happyelements\\Downloads\\zookeeper-3.4.13.tar.gz";
    //    System.out.println(FileMD5.getFileMD5(new File(path)));
    //}
    //
    ///**
    // * 根据文件计算出文件的MD5
    // *
    // * @param file
    // * @return
    // */
    //public static String getFileMD5(File file) {
    //    if (!file.isFile()) {
    //        return null;
    //    }
    //
    //    MessageDigest digest = null;
    //    FileInputStream in = null;
    //    byte buffer[] = new byte[1024];
    //    int len;
    //    try {
    //        digest = MessageDigest.getInstance("MD5");
    //        in = new FileInputStream(file);
    //        while ((len = in.read(buffer, 0, 1024)) != -1) {
    //            digest.update(buffer, 0, len);
    //        }
    //        in.close();
    //
    //    } catch (NoSuchAlgorithmException e) {
    //        e.printStackTrace();
    //    } catch (FileNotFoundException e) {
    //        e.printStackTrace();
    //    } catch (IOException e) {
    //        e.printStackTrace();
    //    }
    //    BigInteger bigInt = new BigInteger(1, digest.digest());
    //
    //    return bigInt.toString(16);
    //}
    //
    ///**
    // * 获取文件夹中的文件的MD5值
    // *
    // * @param file
    // * @param listChild
    // * @return
    // */
    //public static Map<String, String> getDirMD5(File file, boolean listChild) {
    //    if (!file.isDirectory()) {
    //        return null;
    //    }
    //
    //    Map<String, String> map = new HashMap<String, String>();
    //    String md5;
    //
    //    File[] files = file.listFiles();
    //    for (int i = 0; i < files.length; i++) {
    //        File file2 = files[i];
    //        if (file2.isDirectory() && listChild) {
    //            map.putAll(getDirMD5(file2, listChild));
    //        } else {
    //            md5 = getFileMD5(file2);
    //            if (md5 != null) {
    //                map.put(file2.getPath(), md5);
    //            }
    //        }
    //    }
    //    return map;
    //}

    private static final char[] hexDigits = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            'a', 'b', 'c', 'd', 'e', 'f'};

    public static void main(String[] args) {
        //此处我测试的是我本机jdk源码文件的MD5值
        String filePath = "C:\\Users\\happyelements\\Downloads\\zookeeper-3.4.13.tar.gz";
        String md5Hashcode2 = getFileMD5(new File(filePath));

        System.out.println("计算文件md5值为：" + md5Hashcode2);
        System.out.println("计算文件md5值的长度为：" + md5Hashcode2.length());
    }

    /**
     * Get MD5 of a file (lower case)
     *
     * @return empty string if I/O error when get MD5
     */
    public static String getFileMD5(File file) {

        FileInputStream in = null;
        try {
            in = new FileInputStream(file);
            FileChannel ch = in.getChannel();
            return MD5(ch.map(FileChannel.MapMode.READ_ONLY, 0, file.length()));
        } catch (FileNotFoundException e) {
            return "";
        } catch (IOException e) {
            return "";
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    // 关闭流产生的错误一般都可以忽略
                }
            }
        }

    }

    /**
     * MD5校验字符串
     *
     * @param s String to be MD5
     * @return 'null' if cannot get MessageDigest
     */

    private static String getStringMD5(String s) {
        MessageDigest mdInst;
        try {
            // 获得MD5摘要算法的 MessageDigest 对象
            mdInst = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return "";
        }

        byte[] btInput = s.getBytes();
        // 使用指定的字节更新摘要
        mdInst.update(btInput);
        // 获得密文
        byte[] md = mdInst.digest();
        // 把密文转换成十六进制的字符串形式
        int length = md.length;
        char[] str = new char[length * 2];
        int k = 0;
        for (byte b : md) {
            str[k++] = hexDigits[b >>> 4 & 0xf];
            str[k++] = hexDigits[b & 0xf];
        }
        return new String(str);
    }


    @SuppressWarnings("unused")
    private static String getSubStr(String str, int subNu, char replace) {
        int length = str.length();
        if (length > subNu) {
            str = str.substring(length - subNu, length);
        } else if (length < subNu) {
            // NOTE: padding字符填充在字符串的右侧，和服务器的算法是一致的
            str += createPaddingString(subNu - length, replace);
        }
        return str;
    }


    private static String createPaddingString(int n, char pad) {
        if (n <= 0) {
            return "";
        }

        char[] paddingArray = new char[n];
        Arrays.fill(paddingArray, pad);
        return new String(paddingArray);
    }

    /**
     * 计算MD5校验
     *
     * @param buffer
     * @return 空串，如果无法获得 MessageDigest实例
     */

    private static String MD5(ByteBuffer buffer) {
        String s = "";
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(buffer);
            /*
            MD5 的计算结果是一个 128 位的长整数，用字节表示就是 16 个字节
             */
            byte[] tmp = md.digest();
            /*
            每个字节用 16 进制表示的话，使用两个字符，所以表示成 16 进制需要 32 个字符
             */
            char[] str = new char[16 * 2];
            /*
            表示转换结果中对应的字符位置
             */
            int k = 0;
            /*
            从第一个字节开始，对 MD5 的每一个字节
             */
            for (int i = 0; i < 16; i++) {
                // 转换成 16 进制字符的转换
                // 取第 i 个字节
                byte byte0 = tmp[i];
                /*
                取字节中高 4 位的数字转换, >>>,逻辑右移，将符号位一起右移
                 */
                str[k++] = hexDigits[byte0 >>> 4 & 0xf];
                /*
                取字节中低 4 位的数字转换
                 */
                str[k++] = hexDigits[byte0 & 0xf];
            }
            /*
            换后的结果转换为字符串
             */
            s = new String(str);

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return s;
    }


}