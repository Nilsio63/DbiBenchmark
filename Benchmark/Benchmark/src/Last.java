import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Last extends Thread
{
	private static Connection con;
	
	private static final java.util.SplittableRandom random = new java.util.SplittableRandom();
	
	private PreparedStatement prepStmtKontostand = null;
	
	private PreparedStatement prepStmtEinzahlungAccount = null;
	private PreparedStatement prepStmtEinzahlungHistory = null;
	private PreparedStatement prepStmtEinzahlungTellers = null;
	private PreparedStatement prepStmtEinzahlungBranches = null;
	
	private PreparedStatement prepStmtAnalyse = null;
	
	public int anzahl;
	public double tps;
	
	public static void init()
		throws SQLException
	{
		con = getConnection();
		
		clearHistory();
	}
	
	private static void clearHistory()
		throws SQLException
	{
		Statement stmt = con.createStatement();
		
		stmt.execute("delete from history");
		
		stmt.close();
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
        
        con.setTransactionIsolation(1);

        return con;
    }
	
	@Override
	public void run()
	{
		try
		{
			executePhase("Einschwingphase", 4 * 60);
			executePhase("Messphase", 5 * 60, true);
			executePhase("Auslaufphase", 1 * 60);
			
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
	}
	
	private void initPreparedStatements()
		throws SQLException
	{
		prepStmtKontostand = con.prepareStatement("select balance from accounts where accid = ?");
		
		prepStmtEinzahlungAccount = con.prepareStatement("update accounts set balance = balance + ? where accid = ?");
		prepStmtEinzahlungHistory = con.prepareStatement("insert into history"
				+ " (accid, tellerid, delta, branchid, accbalance, cmmnt) values (?, ?, ?, ?, ?, ?)");
		prepStmtEinzahlungTellers = con.prepareStatement("update tellers set balance = balance + ? where tellerid = ?");
		prepStmtEinzahlungBranches = con.prepareStatement("update branches set balance = balance + ? where branchid = ?");
		
		prepStmtAnalyse = con.prepareStatement("select count(*) as anz from history where delta = ?");
	}
	
	private void closePreparedStatements()
		throws SQLException
	{
		prepStmtKontostand.close();
		
		prepStmtEinzahlungAccount.close();
		prepStmtEinzahlungHistory.close();
		prepStmtEinzahlungTellers.close();
		prepStmtEinzahlungBranches.close();
		
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
		prepStmtEinzahlungAccount.setInt(1, delta);
		prepStmtEinzahlungAccount.setInt(2, accid);
		prepStmtEinzahlungAccount.execute();
		
		int accbalance = kontostand(accid);

		prepStmtEinzahlungHistory.setInt(1, accid);
		prepStmtEinzahlungHistory.setInt(2, tellerid);
		prepStmtEinzahlungHistory.setInt(3, delta);
		prepStmtEinzahlungHistory.setInt(4, branchid);
		prepStmtEinzahlungHistory.setInt(5, accbalance);
		prepStmtEinzahlungHistory.setString(6, "");
		prepStmtEinzahlungHistory.execute();
		
		prepStmtEinzahlungTellers.setInt(1, delta);
		prepStmtEinzahlungTellers.setInt(2, tellerid);
		prepStmtEinzahlungTellers.execute();
		
		prepStmtEinzahlungBranches.setInt(1, delta);
		prepStmtEinzahlungBranches.setInt(2, branchid);
		prepStmtEinzahlungBranches.execute();

		return accbalance;
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
