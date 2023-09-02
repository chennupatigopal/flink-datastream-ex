package com.practice.flink.sink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SimpleJDBCConnectionTest {
	static Connection conn = null;
	static PreparedStatement PrepareStat = null;
	public static void main(String[] args) throws SQLException {
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("Sorry, couldn't found JDBC driver. Make sure you have added JDBC Maven Dependency Correctly");
			e.printStackTrace();
			return;
		}

		try {
			// DriverManager: The basic service for managing a set of JDBC drivers.
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/local_scimdb", "scim-user", "scim1234");
			if (conn != null) {
				System.out.println("Connection Successful! Enjoy. Now it's time to push data");
			} else {
				System.out.println("Failed to make connection!");
			}
		} catch (SQLException e) {
			System.out.println("MySQL Connection Failed!");
			e.printStackTrace();
			return;
		}

		String insertQueryStatement = "INSERT  INTO  OrgCount  VALUES  (?,?) ON DUPLICATE KEY UPDATE count = ?";

		PrepareStat = conn.prepareStatement(insertQueryStatement);
	}
	

}
