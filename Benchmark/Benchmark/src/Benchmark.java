import java.io.*;
import java.sql.*;

public class Benchmark
{
	protected static Connection getConnection() throws SQLException
	{
		try
		{
			return DriverManager.getConnection("jdbc:sqlserver://192.168.230.132;DatabaseName=Benchmark;", "dbi", "dbi_pass");
		}
		catch (SQLException e)
		{
			throw new SQLException("JDBC driver not found! " + e);
		}
	}
	
	public static void main(String[] args) 
	{
		try 
		{
			Connection conn = getConnection();
			System.out.println("Connection erfolgreich erstellt!");
		} 
		catch (SQLException e) 
		{
			System.err.println(e);
			System.exit(1);
		}
	}
}