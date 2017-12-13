import java.util.*;
import java.sql.*;

/**
 * Programm zur Erstellung von den Tabellen "Accounts", "Branches", "Tellers" und "History" und
 * Insterts in die einzelnen Tabellen in abhängigkeit von einem Übergabeparameter "n" durhc den User.
 * @author Leon Arndt, Nils Balke, Alina Wewering
 * @version 6.0
 */
public class Benchmark
{
	private static final Scanner input = new Scanner(System.in);
	
	private static final java.util.SplittableRandom random = new java.util.SplittableRandom();
	
	private static Statement stmt;
	private static Connection con;

    public static void main(String[] args)
    {
        System.out.print("n: ");
        int n = input.nextInt();

        createDB(n);
    }
    
    /**
     * Funktion, welche alle Unterfunktionen zur Erstellung der DB und dem Insert in die DB aufruft.
     * @param n Parameter, der für die Anzahl an Inserts verantwortlich ist.
     */
    private static void createDB(int n)
    {
        try
        {
            dropTables();

            createTables();
            
            System.out.println("Zeitmessung startet!");
            
            long timePrevious = System.currentTimeMillis();
            
            insertToTables(n);

            long timeSpan = System.currentTimeMillis() - timePrevious;
            
            System.out.println("Benötigte Zeit in s: " + (double)timeSpan / 1000);
        }
        catch (SQLException ex)
        {
            System.out.println("SqlException: " + ex.getMessage());
        }
    }
    
    /**
     * Funktion, welche alle Tabellen in der DB löscht.
     * @throws SQLException Fehlermeldungen werden nach oben durchgereicht.
     */
    private static void dropTables()
    	throws SQLException
    {
    	buildConnection();
    	
    	dropTable("history");
    	dropTable("tellers");
    	dropTable("accounts");
    	dropTable("branches");

    	commit();
    }
    
    /**
     * Funktion, welche eine Tabelle löscht.
     * @param tableName Name der zu löschenden Tabelle
     * @throws SQLException Fehlermeldungen werden nach oben durchgereicht.
     */
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
    
    /**
     * Funktion, welche alle Tabellen erstellt.
     * @throws SQLException Fehlermeldungen werden nach oben durchgereicht.
     */
    private static void createTables()
    	throws SQLException
    {
    	buildConnection();

	    stmt.execute("create table branches" +
	            "( branchid int not null," +
	            " branchname char(20) not null," +
	            " balance int not null," +
	            " address char(72) not null );");
	    
	    stmt.execute("create table accounts" + 
	    		"( accid int not null," + 
	    		" name char(20) not null," + 
	    		" balance int not null," + 
	    		" branchid int not null," + 
	    		" address char(68) not null ); ");
	    
	    stmt.execute("create table tellers" + 
	    		"( tellerid int not null," + 
	    		" tellername char(20) not null," + 
	    		" balance int not null," + 
	    		" branchid int not null," + 
	    		" address char(68) not null );");
	    
	    stmt.execute("create table history" + 
	    		"( accid int not null," + 
	    		" tellerid int not null," + 
	    		" delta int not null," + 
	    		" branchid int not null," + 
	    		" accbalance int not null," + 
	    		" cmmnt char(30) not null ); ");
	    
	    commit();
    }
    
    /**
     * Funktion, die alle Insert-Funktionen aufruft.
     * @param n Parameter, der für die Anzahl an Inserts verantwortlich ist.
     * @throws SQLException Fehlermeldungen werden nach oben durchgereicht.
     */
    private static void insertToTables(int n)
    	throws SQLException
    {
    	buildConnection();
    	
    	insertBranches(n);
    	
    	insertAccounts(n);
    	
    	insertTellers(n);
    	
    	addKeys();

    	commit();
    }
    
    /**
     * Insertfunktion in die Tabelle Branches.
     * @param n Parameter, der für die Anzahl an Inserts verantwortlich ist.
     * @throws SQLException Fehlermeldungen werden nach oben durchgereicht.
     */
    private static void insertBranches(int n)
    	throws SQLException
    {
    	PreparedStatement prepStmt = con.prepareStatement("insert into branches (branchid, branchname, balance, address)"
    			+ " values (?,"
				+ "?,"
				+ "?,"
				+ "?)");
    	
    	// For-Schleife mit n Durchläufen.
    	// Setzen der jeweiligen Parameter für das PreparedStatement.
    	// Hinzufügen des jeweiligen batches.
    	for (int i = 1; i <= n; i++)
    	{
    		prepStmt.setInt(1, i);
    		prepStmt.setString(2, "aaaaaaaaaaaaaaaaaaaa");
    		prepStmt.setInt(3, 0);
    		prepStmt.setString(4, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    		
    		prepStmt.addBatch();
    	}
    	
    	// Ausführen der Batches
    	prepStmt.executeBatch();
    	prepStmt.close();
    }
    
    /**
     * Insertfunktion in die Tabelle Accounts.
     * @param n Parameter, der für die Anzahl an Inserts verantwortlich ist.
     * @throws SQLException Fehlermeldungen werden nach oben durchgereicht.
     */
    private static void insertAccounts(int n)
    	throws SQLException
    {
    	PreparedStatement prepStmt = con.prepareStatement("insert into accounts (accid, name, balance, branchid, address)"
					+ " values (?,"
					+ "?,"
					+ "?,"
					+ "?,"
					+ "?)");
    	
    	// For-Schleife mit n*100000 Durchläufen.
    	// Setzen der jeweiligen Parameter für das PreparedStatement.
    	// Hinzufügen des jeweiligen batches.
    	for (long i = 1; i <= ((long)n * 100000); i++)
    	{
    		prepStmt.setLong(1,  i);
    		prepStmt.setString(2, "aaaaaaaaaaaaaaaaaaaa");
    		prepStmt.setInt(3, 0);
    		prepStmt.setInt(4, random.nextInt(n) + 1);
    		prepStmt.setString(5, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

    		prepStmt.addBatch();
    		
    		// Ausführen der Batches nach jeweils 2500 Statements
    		if (i % 2500 == 0)
    		{
    			prepStmt.executeBatch();
    		}
    	}
    	
    	prepStmt.close();
    }
    
    /**
     * Insertfunktion in die Tabelle Tellers.
     * @param n Parameter, der für die Anzahl an Inserts verantwortlich ist.
     * @throws SQLException Fehlermeldungen werden nach oben durchgereicht.
     */
    private static void insertTellers(int n)
    	throws SQLException
    {
    	PreparedStatement prepStmt = con.prepareStatement("insert into tellers (tellerid, tellername, balance, branchid, address)"
    				+ " values (?,"
    				+ "?,"
    				+ "?,"
    				+ "?,"
    				+ "?)");
    	
    	// For-Schleife mit n*10 Durchläufen.
    	// Setzen der jeweiligen Parameter für das PreparedStatement.
    	// Hinzufügen des jeweiligen batches.
    	for (int i = 1; i <= n * 10; i++)
    	{
    		prepStmt.setInt(1, i);
    		prepStmt.setString(2, "aaaaaaaaaaaaaaaaaaaa");
    		prepStmt.setInt(3, 0);
    		prepStmt.setInt(4, random.nextInt(n) + 1);
    		prepStmt.setString(5, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    		
    		prepStmt.addBatch();
    	}
    	
    	// Ausführen der Batches
    	prepStmt.executeBatch();
    	prepStmt.close();
    }
    
    /**
     * Funktion zum setzen der Primär- und Fremdschlüsselparameter in den einzelnen Tabellen.
     * @throws SQLException Fehlermeldungen werden nach oben durchgereicht.
     */
    private static void addKeys()
    	throws SQLException
    {
    	// Tabelle Branches
    	stmt.addBatch("alter table branches"
    			+ " add primary key (branchid)");
    	
    	// Tabelle Accounts
    	stmt.addBatch("alter table accounts"
    			+ " add primary key (accid)");
    	stmt.addBatch("alter table accounts"
    			+ " add foreign key (branchid) references branches (branchid)");
    	
    	// Tabelle Tellers
    	stmt.addBatch("alter table tellers"
    			+ " add primary key (tellerid)");
    	stmt.addBatch("alter table tellers"
    			+ " add foreign key (branchid) references branches (branchid)");
    	
    	// Tabelle History
    	stmt.addBatch("alter table history"
    			+ " add foreign key (accid) references accounts (accid)");
    	stmt.addBatch("alter table history"
    			+ " add foreign key (tellerid) references tellers (tellerid)");
    	stmt.addBatch("alter table history"
    			+ " add foreign key (branchid) references branches (branchid)");
    	
    	// Ausführen der Batches
    	stmt.executeBatch();
    }
    
    /**
     * Funktion zur Speicherung der Verbindungsdaten in lokale Variablen.
     * @throws SQLException Fehlermeldungen werden nach oben durchgereicht.
     */
    private static void buildConnection()
    	throws SQLException
    {
    	con = getConnection();
    	
    	stmt = con.createStatement();
    }
    
    /**
     * Funktion zur Ausführung der aktuellen Verbindung.
     * @throws SQLException Fehlermeldungen werden nach oben durchgereicht.
     */
    private static void commit()
    	throws SQLException
    {
    	stmt.close();
    	
    	stmt = null;
    	
    	con.commit();
    	
    	con = null;
    }

    /**
     * Funktion zur Erstellung einer neunen Instance einer neuen Verbindung zur DB.
     * @return Gibt ein Connectionobjekt zurück.
     * @throws SQLException Fehlermeldungen werden nach oben durchgereicht.
     */
    private static Connection getConnection()
        	throws SQLException
    {
        Connection con = DriverManager.getConnection("jdbc:sqlserver://192.168.122.30;databaseName=Benchmark;",
                "dbi", "dbi_pass");

        con.setAutoCommit(false);

        return con;
    }
    
}