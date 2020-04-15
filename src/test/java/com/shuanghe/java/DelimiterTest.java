package com.shuanghe.java;

import org.junit.Test;

/**
 * Description:
 * <p>
 * Date: 2018/09/30
 * Time: 17:02
 *
 * @author Shuanghe Yu
 */
public class DelimiterTest {
    @Test
    public void test1() {
        System.out.println("\001");
        System.out.println("\002");
        System.out.println("\003");
        System.out.println("\\001");
        System.out.println("\t");
    }
}