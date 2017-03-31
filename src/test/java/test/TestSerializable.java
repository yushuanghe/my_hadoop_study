package test;

import java.io.Serializable;

/**
 * Created by yushuanghe on 2017/03/30.
 */
public class TestSerializable implements Serializable {

    private static final long serialVersionUID = -7790703269971346150L;

    public static void main(String[] args) {
        System.out.println("大力出奇迹！");
        System.out.println(new String("abc").equals(new String("abc")));
        System.out.println(new StringBuffer("abc").equals(new StringBuffer("abc")));
        System.out.println("-----我是分割线-----");

        String a = "abcd";
        String b = "ab" + "cd";
        System.out.println(a == b);//true
        String t = "cd";
        String c = "ab" + t;
        System.out.println(a == c);//false
        final String s = "cd";
        String d = "ab" + s;
        System.out.println(a == d);//true
        String r = "ab";
        String e = r + t;
        System.out.println(a == e);//false
        System.out.println(e.intern() == a);//true
        String f = new String("abcd");
        System.out.println(a == f);//false
        System.out.println(f.intern() == a);//true
        System.out.println(f.intern() == a.intern());//true
    }
}
