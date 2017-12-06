import java.util.*;
import java.sql.*;

public class Benchmark
{
	private static final Scanner input = new Scanner(System.in);
	
	private static Connection con;

    public static void main(String[] args)
    {
        System.out.print("n: ");
        int n = input.nextInt();

        createDB(n);;
    }

    private static void createDB(int n)
    {
        try
        {
            con = getConnection();
            
            dropTables();

            createTables();
            
            long timePrevious = System.currentTimeMillis();
            
            insertToTables(n);
            
            long timeSpan = System.currentTimeMillis() - timePrevious;
            
            System.out.println("Benötigte Zeit in s: " + (double)timeSpan / 1000);

            con.commit();
        }
        catch (SQLException ex)
        {
            System.out.println("SqlException: " + ex.getMessage());
        }
    }
    
    private static void dropTables()
    	throws SQLException
    {
    	dropTable("history");
    	dropTable("tellers");
    	dropTable("accounts");
    	dropTable("branches");
    }
    
    private static void dropTable(String tableName)
    	throws SQLException
    {
    	Statement stmt = con.createStatement();
    	
    	stmt.execute("if (exists (select *"
        		+ " from information_schema.tables"
        		+ " where table_name = '" + tableName + "'))"
        		+ " begin "
        		+ " drop table " + tableName + ";"
        		+ " end");
    }
    
    private static void createTables()
    	throws SQLException
    {
    	Statement x = con.createStatement();

	    x.execute("create table branches" +
	            "( branchid int not null," +
	            " branchname char(20) not null," +
	            " balance int not null," +
	            " address char(72) not null," +
	            " primary key (branchid) );");
	    
	    x.execute("create table accounts" + 
	    		"( accid int not null," + 
	    		" name char(20) not null," + 
	    		" balance int not null," + 
	    		" branchid int not null," + 
	    		" address char(68) not null," + 
	    		" primary key (accid)," + 
	    		" foreign key (branchid) references branches ); ");
	    
	    x.execute("create table tellers" + 
	    		"( tellerid int not null," + 
	    		" tellername char(20) not null," + 
	    		" balance int not null," + 
	    		" branchid int not null," + 
	    		" address char(68) not null," + 
	    		" primary key (tellerid)," + 
	    		" foreign key (branchid) references branches );");
	    
	    x.execute("create table history" + 
	    		"( accid int not null," + 
	    		" tellerid int not null," + 
	    		" delta int not null," + 
	    		" branchid int not null," + 
	    		" accbalance int not null," + 
	    		" cmmnt char(30) not null," + 
	    		" foreign key (accid) references accounts," + 
	    		" foreign key (tellerid) references tellers," + 
	    		" foreign key (branchid) references branches ); ");
    }
    
    private static void insertToTables(int n)
    	throws SQLException
    {
    	insertBranches(n);
    	
    	insertAccounts(n);
    	
    	insertTellers(n);
    }
    
    private static void insertBranches(int n)
    	throws SQLException
    {
    	for (int i = 0; i < n; i++)
    	{
    		Statement stmt = con.createStatement();
    		
    		stmt.execute("insert into branches (branchid, branchname, balance, address)"
    				+ " values (" + (i + 1) + ","
    				+ "'aaaaaaaaaaaaaaaaaaaa',"
    				+ "0,"
    				+ "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')");
    		
//    		System.out.println("insert into branches (branchid, branchname, balance, address)"
//    				+ " values (" + (i + 1) + ","
//    				+ "'aaaaaaaaaaaaaaaaaaaa',"
//    				+ "0,"
//    				+ "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')");
    	}
    }
    
    private static void insertAccounts(int n)
    	throws SQLException
    {
    	for (long i = 0; i < ((long)n * 100000); i++)
    	{
			Statement stmt = con.createStatement();
			
			stmt.execute("insert into accounts (accid, name, balance, branchid, address)"
					+ " values (" + (i + 1) + ","
					+ "'aaaaaaaaaaaaaaaaaaaa',"
					+ "0,"
					+ ((int)(Math.random() * n) + 1) + ","
					+ "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')");
    		
//    		System.out.println("insert into accounts (accid, name, balance, branchid, address)"
//					+ " values (" + (i + 1) + ","
//					+ "'aaaaaaaaaaaaaaaaaaaa',"
//					+ "0,"
//					+ ((int)(Math.random() * n) + 1) + ","
//					+ "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')");
    	}
    }
    
    private static void insertTellers(int n)
    	throws SQLException
    {
    	for (int i = 0; i < n * 10; i++)
    	{
    		Statement stmt = con.createStatement();
    		
    		stmt.execute("insert into tellers (tellerid, tellername, balance, branchid, address)"
    				+ " values (" + (i + 1) + ","
    				+ "'aaaaaaaaaaaaaaaaaaaa',"
    				+ "0,"
    				+ ((int)(Math.random() * n) + 1) + ","
    				+ "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')");
    		
//    		System.out.println("insert into tellers (tellerid, tellername, balance, branchid, address)"
//    				+ " values (" + (i + 1) + ","
//    				+ "'aaaaaaaaaaaaaaaaaaaa',"
//    				+ "0,"
//    				+ ((int)(Math.random() * n) + 1) + ","
//    				+ "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')");
    	}
    }

    private static Connection getConnection()
            throws SQLException
    {
        try
        {
            Connection con = DriverManager.getConnection("jdbc:sqlserver://192.168.230.132;databaseName=Benchmark;",
                    "dbi", "dbi_pass");

            con.setAutoCommit(false);

            return con;
        }
        catch (SQLException e)
        {
            throw new SQLException("JDBC driver not found!" + e.getMessage());
        }
    }
    
}