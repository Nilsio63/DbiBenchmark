import java.util.*;
import java.sql.*;

public class Benchmark
{
	private static final Scanner input = new Scanner(System.in);
	private static Statement stmt;
	
	private static Connection con;

    public static void main(String[] args)
    {
        System.out.print("n: ");
        int n = input.nextInt();

        createDB(n);
    }

    private static void createDB(int n)
    {
        try
        {
            con = getConnection();
            
            stmt = con.createStatement();
            
            dropTables();

            createTables();
            
            long timePrevious = System.currentTimeMillis();
            
            insertToTables(n);

            con.commit();
            
            long timeSpan = System.currentTimeMillis() - timePrevious;
            
            System.out.println("Benötigte Zeit in s: " + (double)timeSpan / 1000);
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

	    stmt.execute("create table branches" +
	            "( branchid int not null," +
	            " branchname char(20) not null," +
	            " balance int not null," +
	            " address char(72) not null," +
	            " primary key (branchid) );");
	    
	    stmt.execute("create table accounts" + 
	    		"( accid int not null," + 
	    		" name char(20) not null," + 
	    		" balance int not null," + 
	    		" branchid int not null," + 
	    		" address char(68) not null," + 
	    		" primary key (accid)," + 
	    		" foreign key (branchid) references branches ); ");
	    
	    stmt.execute("create table tellers" + 
	    		"( tellerid int not null," + 
	    		" tellername char(20) not null," + 
	    		" balance int not null," + 
	    		" branchid int not null," + 
	    		" address char(68) not null," + 
	    		" primary key (tellerid)," + 
	    		" foreign key (branchid) references branches );");
	    
	    stmt.execute("create table history" + 
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
    	PreparedStatement prepStmt = con.prepareStatement("insert into branches (branchid, branchname, balance, address)"
    			+ " values (?,"
				+ "'aaaaaaaaaaaaaaaaaaaa',"
				+ "0,"
				+ "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')");
    	
    	for (int i = 0; i < n; i++)
    	{
    		prepStmt.setInt(1, i + 1);
    		
    		prepStmt.executeUpdate();
    		
//    		stmt.execute("insert into branches (branchid, branchname, balance, address)"
//    				+ " values (" + (i + 1) + ","
//    				+ "'aaaaaaaaaaaaaaaaaaaa',"
//    				+ "0,"
//    				+ "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')");
    		
//    		System.out.println("insert into branches (branchid, branchname, balance, address)"
//    				+ " values (" + (i + 1) + ","
//    				+ "'aaaaaaaaaaaaaaaaaaaa',"
//    				+ "0,"
//    				+ "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')");
    	}
    	
    	prepStmt.close();
    }
    
    private static void insertAccounts(int n)
    	throws SQLException
    {
    	PreparedStatement prepStmt = con.prepareStatement("insert into accounts (accid, name, balance, branchid, address)"
					+ " values (?,"
					+ "'aaaaaaaaaaaaaaaaaaaa',"
					+ "0,"
					+ "?,"
					+ "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')");
    	
    	for (long i = 0; i < ((long)n * 100000); i++)
    	{
    		prepStmt.setLong(1,  i + 1);
    		prepStmt.setInt(2, ((int)(Math.random() * n) + 1));
    		
    		prepStmt.executeUpdate();
    		
//			stmt.execute("insert into accounts (accid, name, balance, branchid, address)"
//					+ " values (" + (i + 1) + ","
//					+ "'aaaaaaaaaaaaaaaaaaaa',"
//					+ "0,"
//					+ ((int)(Math.random() * n) + 1) + ","
//					+ "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')");
    		
//    		System.out.println("insert into accounts (accid, name, balance, branchid, address)"
//					+ " values (" + (i + 1) + ","
//					+ "'aaaaaaaaaaaaaaaaaaaa',"
//					+ "0,"
//					+ ((int)(Math.random() * n) + 1) + ","
//					+ "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')");
    	}
    	
    	prepStmt.close();
    }
    
    private static void insertTellers(int n)
    	throws SQLException
    {
    	PreparedStatement prepStmt = con.prepareStatement("insert into tellers (tellerid, tellername, balance, branchid, address)"
    				+ " values (?,"
    				+ "'aaaaaaaaaaaaaaaaaaaa',"
    				+ "0,"
    				+ "?,"
    				+ "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')");
    	
    	for (int i = 0; i < n * 10; i++)
    	{    		
    		prepStmt.setInt(1, i + 1);
    		prepStmt.setInt(2, ((int)(Math.random() * n) + 1));
    		
    		prepStmt.executeUpdate();
    		
//    		stmt.execute("insert into tellers (tellerid, tellername, balance, branchid, address)"
//    				+ " values (" + (i + 1) + ","
//    				+ "'aaaaaaaaaaaaaaaaaaaa',"
//    				+ "0,"
//    				+ ((int)(Math.random() * n) + 1) + ","
//    				+ "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')");
    		
//    		System.out.println("insert into tellers (tellerid, tellername, balance, branchid, address)"
//    				+ " values (" + (i + 1) + ","
//    				+ "'aaaaaaaaaaaaaaaaaaaa',"
//    				+ "0,"
//    				+ ((int)(Math.random() * n) + 1) + ","
//    				+ "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')");
    	}
    	
    	prepStmt.close();
    }

    private static Connection getConnection()
    		throws SQLException
    {
        Connection con = DriverManager.getConnection("jdbc:sqlserver://192.168.122.30;databaseName=Benchmark;",
                "dbi", "dbi_pass");

        con.setAutoCommit(false);

        return con;
    }
    
}