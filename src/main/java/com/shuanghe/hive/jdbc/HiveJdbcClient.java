package com.shuanghe.hive.jdbc;

import java.sql.*;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-2-2
 * Time: 下午2:01
 * To change this template use File | Settings | File Templates.
 * Description:
 */
public class HiveJdbcClient {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    /**
     * @param args
     * @throws SQLException
     */
    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        //replace "hive" here with the name of the user the queries should run as
        Connection con = DriverManager.getConnection("jdbc:hive2://shuanghe.com:10000/test", "yushuanghe", "123456");

        Statement state = con.createStatement();
        String tableName = "testHiveDriverTable";
        state.execute("drop table if exists " + tableName);
        String sql="create table " + tableName + " (key int, value string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'";
        state.execute(sql);
        System.out.println("Running: " + sql);

        // show tables
        sql = "show tables '" + tableName + "'";
        System.out.println("Running: " + sql);
        ResultSet rs = state.executeQuery(sql);
        if (rs.next()) {
            System.out.println(rs.getString(1));
        }

        // describe table
        sql = "describe " + tableName;
        System.out.println("Running: " + sql);
        rs = state.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1) + "\t" + rs.getString(2));
        }

        // load data into table
        // NOTE: filepath has to be local to the hive server
        // NOTE: /tmp/a.txt is a ctrl-A separated file with two fields per line
        String filepath = "/home/yushuanghe/test/student.txt";
        sql = "load data local inpath '" + filepath + "' into table " + tableName;
        System.out.println("Running: " + sql);
        state.execute(sql);

        // select * query
        sql = "select * from " + tableName;
        System.out.println("Running: " + sql);
        rs = state.executeQuery(sql);
        while (rs.next()) {
            System.out.println(String.valueOf(rs.getInt(1)) + "\t" + rs.getString(2));
        }

        // regular hive query
        sql = "select count(1) from " + tableName;
        System.out.println("Running: " + sql);
        rs = state.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
    }
}