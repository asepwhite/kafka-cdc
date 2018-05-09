package com.akbarseptriyan.app;
import java.sql.*;

public class TestSQL {

    static final String JBDC_DRIVER = "org.postgresql.Driver";
    static final String DB_HOST = "jdbc:postgresql://localhost:5432/debezdb";
    static final String DB_USER = "debezium";
    static final String DB_PASSWORD = "dbz12345";

    public static void main(String args[]) throws SQLException {
        Connection conn  = DriverManager.getConnection(DB_HOST, DB_USER, DB_PASSWORD);
        Statement stmnt = conn.createStatement();


//        String sql = "insert into animals(type, sex) values('repltilenewbaru', 'male')";
//        stmnt.executeUpdate(sql);

//        String sql = "select count (type) as total from animals where type = 'mamals'";
//        ResultSet rs = stmnt.executeQuery(sql);
//        rs.next();
//        System.out.println(rs.getInt("total"));
        String sql = "select * from animals where id = '100'";
        ResultSet rs =  stmnt.executeQuery(sql);
        System.out.println(rs.next());
        while (rs.next()){
            System.out.println(rs.getString("type"));
            System.out.println(rs.getString("type") == null);
        }


        stmnt.close();
        conn.close();
    }
}
