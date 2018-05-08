package com.shuanghe.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-5-9
 * Time: 上午1:12
 * To change this template use File | Settings | File Templates.
 * Description:简易版连接池
 */
public class ConnectionPool {
    //静态的 Connection 队列
    private static LinkedList<Connection> connectionQueue;

    //加载驱动
    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public synchronized static Connection getConnection() {
        try {
            if (connectionQueue == null) {
                connectionQueue = new LinkedList<>();

                for (int i = 0; i < 5; i++) {
                    Connection conn = null;

                    conn = DriverManager.getConnection(
                            "jdbc:mysql://127.0.0.1:3306/test",
                            "root",
                            "123456");

                    connectionQueue.push(conn);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connectionQueue.poll();
    }

    public synchronized static void returnConnection(Connection conn) {
        connectionQueue.push(conn);
    }
}