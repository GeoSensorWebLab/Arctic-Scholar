package main;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class agtDB {

	public Connection conn = null;
	 
	public agtDB() {
		Properties prop = new Properties();
		InputStream input = null;
		try {
			input = new FileInputStream("config.properties");
			prop.load(input);
			Class.forName(prop.getProperty("JDBCdriver"));
			String url = prop.getProperty("dbagtUrl");
			conn = DriverManager.getConnection(url, prop.getProperty("localUser"), prop.getProperty("localPassword"));
			//System.out.println("conn built");
		} catch (IOException ex) {
			ex.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
 
	
	
	public ResultSet runSqlResultSet(String sql) throws SQLException {
		Statement sta = conn.createStatement();
		return sta.executeQuery(sql);
	}
	
	public void runSql(String sql) throws SQLException {
		Statement sta = conn.createStatement();
		sta.executeUpdate(sql);
		//return sta.executeQuery(sql);
	}
 
	public boolean runSql2(String sql) throws SQLException {
		Statement sta = conn.createStatement();
		return sta.execute(sql);
	}
	
 
	@Override
	protected void finalize() throws Throwable {
		if (conn != null || !conn.isClosed()) {
			conn.close();
			//System.out.println("conn close");
		}
	}
	
}
