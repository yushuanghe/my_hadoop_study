package com.shuanghe.sparkproject.test;

import com.shuanghe.sparkproject.conf.ConfigurationManager;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-5-27
 * Time: 上午12:13
 * To change this template use File | Settings | File Templates.
 * Description:配置管理组件测试类
 *
 * @author yushuanghe
 */
public class ConfigurationManagerTest {
    public static void main(String[] args) {
        String testkey1 = ConfigurationManager.getProperty("testkey1");
        String testkey2 = ConfigurationManager.getProperty("jdbc.url");
        System.out.println(testkey1);
        System.out.println(testkey2);
    }
}