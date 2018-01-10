import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.CallableStatement;

public class Last extends Thread
{
	private static Connection con;
	
	private static final java.util.SplittableRandom random = new java.util.SplittableRandom();
	
	private PreparedStatement prepStmtKontostand = null;
	
	private PreparedStatement prepStmtAnalyse = null;
	
	public int anzahl;
	public double tps;
	
	public static void init()
		throws SQLException
	{
		con = getConnection();
	}
	
	public static void commit()
		throws SQLException
	{
		con.commit();
	}

    /**
     * Funktion zur Erstellung einer neuen Verbindung zur Datenbank.
     * @return Gibt die Insatnz des neuen Verbindungsobjekts zurück.
     * @throws SQLException Fehlermeldungen werden nach oben durchgereicht.
     */
    private static Connection getConnection()
        throws SQLException
    {
        Connection con = DriverManager.getConnection("jdbc:sqlserver://192.168.122.30;databaseName=Benchmark;",
                "dbi", "dbi_pass");

        con.setAutoCommit(false);
        
        con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

        return con;
    }
	
	@Override
	public void run()
	{
		try
		{
			executePhase("Einschwingphase", 4 * 3);
			executePhase("Messphase", 5 * 3, true);
			executePhase("Auslaufphase", 1 * 3);
			
			System.out.println("Anzahl: " + anzahl);
			System.out.println("tps: " + tps);
		}
		catch (SQLException ex)
		{
			System.out.println("SQLException: " + ex.getMessage());
		}
		catch (InterruptedException ex)
		{
			System.out.println("InterruptedException: " + ex.getMessage());
		}
	}
	
	private void executePhase(String phaseName, int timeInSec)
		throws SQLException, InterruptedException
	{
		executePhase(phaseName, timeInSec, false);
	}
	
	private void executePhase(String phaseName, int timeInSec, boolean setValues)
		throws SQLException, InterruptedException
	{
		System.out.println(phaseName + ": " + timeInSec + "s");
		
		int timeSpan = timeInSec * 1000;
		long timeStart = System.currentTimeMillis();
		
		initPreparedStatements();
		
		int count = 0;

		while ((System.currentTimeMillis() - timeStart) < timeSpan)
		{
			executeRandom();
			
			count++;
			
			Thread.sleep(50);
		}
		
		closePreparedStatements();

		if (setValues)
		{
			anzahl = count;
			tps = (double)count / timeInSec;
		}
	}
	
	private void executeRandom()
		throws SQLException
	{
		int rng = random.nextInt(0, 19);
		
		if (rng < 3)
		{
			int delta = random.nextInt(1, 10000);
			int count = analyse(delta);
			
			//System.out.println("analyse(" + delta + ") = " + count);
		}
		else if (rng < 10)
		{
			int accid = random.nextInt(1, 10000000);
			int accbalance = kontostand(accid);
			
			//System.out.println("kontostand(" + accid + ") = " + accbalance);
		}
		else
		{
			int accid = random.nextInt(1, 10000000);
			int tellerid = random.nextInt(1, 1000);
			int branchid = random.nextInt(1, 100);
			int delta = random.nextInt(1, 10000);
			int accbalance = einzahlung(accid, tellerid, branchid, delta);
			
			//System.out.println("einzahlung(" + accid + ", " + tellerid
					//+ ", " + branchid + ", " + delta + ") = " + accbalance);
		}
		
		con.commit();
	}
	
	private void initPreparedStatements()
		throws SQLException
	{
		prepStmtKontostand = con.prepareStatement("select balance from accounts where accid = ?");
		
		prepStmtAnalyse = con.prepareStatement("select count(*) as anz from history where delta = ?");
	}
	
	private void closePreparedStatements()
		throws SQLException
	{
		prepStmtKontostand.close();
		
		prepStmtAnalyse.close();
	}
	
	private int kontostand(int accid)
		throws SQLException
	{
		prepStmtKontostand.setInt(1, accid);
		
		ResultSet set = prepStmtKontostand.executeQuery();
		
		set.next();

		return set.getInt("balance");
	}
	
	private int einzahlung(int accid, int tellerid, int branchid, int delta)
		throws SQLException
	{
		CallableStatement statement = con.prepareCall("{call einzahlung(?,?,?,?,?)}");
		
		statement.setInt(1, accid);
		statement.setInt(2, tellerid);
		statement.setInt(3, branchid);
		statement.setInt(4, delta);
		statement.registerOutParameter(5, java.sql.Types.INTEGER);
		
		statement.executeUpdate();
		
		int balance = statement.getInt(5);
		
		statement.close();
		
		return balance;
	}
	
	private int analyse(int delta)
		throws SQLException
	{
		prepStmtAnalyse.setInt(1, delta);
		
		ResultSet set = prepStmtAnalyse.executeQuery();
		
		set.next();
		
		return set.getInt("anz");
	}
	
}
