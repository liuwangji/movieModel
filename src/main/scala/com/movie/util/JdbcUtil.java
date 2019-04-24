package com.movie.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JdbcUtil {
    public static String driver = "com.mysql.jdbc.Driver";
    public static String url = "jdbc:mysql://localhost:3306/movie_model";
    public static String username = "test";
    public static String password = "123456";

    public static Connection getConn() {
        Connection conn = null;
        try {
            //classLoader,加载对应驱动
            Class.forName(driver);
            conn = (Connection) DriverManager.getConnection(url, username, password);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }
}
