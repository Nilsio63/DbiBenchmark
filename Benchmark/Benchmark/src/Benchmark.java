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
            
            System.out.println("Benötigte Zeit in ms: " + timeSpan);

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
    	Statement stmt = con.createStatement();

        stmt.execute("drop table branches;"
        		+ " drop table accounts;"
        		+ " drop table tellers;"
        		+ " drop table history");
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
	            " primary key (branchid) ); ");
	    
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
    {
    	
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