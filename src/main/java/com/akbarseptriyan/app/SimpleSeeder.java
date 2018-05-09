package com.akbarseptriyan.app;
import java.sql.*;
import java.util.Random;

public class SimpleSeeder {

    private static final String JBDC_DRIVER = "org.postgresql.Driver";
    private static final String DB_HOST = "jdbc:postgresql://localhost:5432/debezdb";

    private static final String DB_USER = "debezium";
    private static final String DB_PASSWORD = "dbz12345";


    public static void main(String[] args){

        Connection conn = null;
        Statement stmt = null;
        String[] names = new String[]{"names 1", "names 2", "names 3", "names 4", "names 5", "names 6", "names 7", "names 8", "names 9", "names 10"};
        String[] adresses = new String[]{"address 1", "address 2", "address 3", "address 4", "address 5", "adress 6", "address 7", "address 8", "address 9", "address 10"};


        try{
            //STEP 2: Register JDBC driver
            Class.forName(JBDC_DRIVER);

            //STEP 3: Open a connection
            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(DB_HOST,DB_USER,DB_PASSWORD);

            System.out.println("Creating statement...");
            stmt = conn.createStatement();

            Random rand = new Random();
            String sql = "";
            for (int i = 1; i < 1000000; i++) {
//                sql = "INSERT INTO PENGGUNA VALUES (2, 'nama', 'alamat')";
                sql = "INSERT INTO PENGGUNA VALUES ("+i+ ", '"+names[rand.nextInt(10)]+"', '"+adresses[rand.nextInt(10)]+"')";
                stmt.executeUpdate(sql);
                System.out.println("Processing data ke "+i);

            }
            stmt.close();
            conn.close();
        }catch(SQLException se){
            //Handle errors for JDBC
            se.printStackTrace();
        }catch(Exception e){
            //Handle errors for Class.forName
            e.printStackTrace();
        }finally{
            //finally block used to close resources
            try{
                if(stmt!=null)
                    stmt.close();
            }catch(SQLException se2){
            }// nothing we can do
            try{
                if(conn!=null)
                    conn.close();
            }catch(SQLException se){
                se.printStackTrace();
            }//end finally try
        }//end try
        System.out.println("Selesai!");
    }
}
