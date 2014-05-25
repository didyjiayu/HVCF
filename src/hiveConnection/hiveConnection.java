/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package hiveConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 *
 * @author yujia1986
 */
public class hiveConnection {
    private static final String driverName = "org.apache.hive.jdbc.HiveDriver";
    public Connection connectHive(String hiveServer, String hiveServerPort, String hiveUser, String hiveUserPassword) throws ClassNotFoundException, SQLException {
        Class.forName(driverName);
        Connection con = DriverManager.getConnection("jdbc:hive2://" + hiveServer + ":" + hiveServerPort + "/default", hiveUser, hiveUserPassword);
        return con;
    }
    
}
