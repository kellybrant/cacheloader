package org.example.cacheloader;

/*
 * 
CREATE TABLE users (
  id    int PRIMARY KEY NOT NULL,
  uuid  CHAR(36)        NOT NULL
);
 * 
 */

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @author Kelly Brant
 *
 */
public class PostgresDao {

    public Map<Integer, String> findAll() {
        Map<Integer, String> dataMap = new HashMap<Integer, String>();
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            Class.forName("org.postgresql.Driver");
            String url = "jdbc:postgresql://localhost/cachedata";
            connection = DriverManager.getConnection(url,"kelly", "");
            connection.setAutoCommit(false);
            statement = connection.createStatement();
            statement.setFetchSize(1000);
            resultSet = statement.executeQuery("select * from users order by id");
            while (resultSet.next()) {
                dataMap.put(resultSet.getInt(1), resultSet.getString(2));
            }
        } catch (ClassNotFoundException e) {
          e.printStackTrace();
          System.exit(1);
        } catch (SQLException e) {
          e.printStackTrace();
          System.exit(2);
        } finally {
            try {
                resultSet.close();
                statement.close();
                connection.close();
            } catch (Exception e) {
                System.out.println("#findAll finally block failed");
            }
        }
        return dataMap;
    }
        
    public void insert(Map<Integer, String> dataMap) {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            Class.forName("org.postgresql.Driver");
            String url = "jdbc:postgresql://localhost/cachedata";
            connection = DriverManager.getConnection(url,"kelly", "");
            statement = connection.prepareStatement("insert into users values (?,?)");

            int counter = 0;
            for (Integer key : dataMap.keySet()) {
                statement.setInt(1, key);
                statement.setString(2, dataMap.get(key));
                statement.addBatch();
                if (counter % 1000 == 0) {
                    statement.executeBatch();
                }
            }
            statement.executeBatch();
        } catch (ClassNotFoundException e) {
          e.printStackTrace();
          System.exit(1);
        } catch (SQLException e) {
          e.printStackTrace();
          System.exit(2);
        } finally {
            try {
                statement.close();
                connection.close();
            } catch (Exception e) {
                System.out.println("#insert finally block failed");
            }
        }
    }
}
