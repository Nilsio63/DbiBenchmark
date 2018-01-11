import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.CallableStatement;

/**
 * Klasse zum Durchführen der Transaktionen einer einzelnen Last.
 * @author Leon Arndt, Nils Balke, Alina Wewering
 * @version 5.0
 */
public class Last extends Thread
{
	private static Connection con;
	
	private static final java.util.SplittableRandom random = new java.util.SplittableRandom();
	
	private PreparedStatement prepStmtKontostand = null;
	private CallableStatement callStmtEinzahlung = null;
	private PreparedStatement prepStmtAnalyse = null;
	
	public int anzahl;
	public double tps;
	
	/**
	 * Initalisiert die statische Verbindung zur Datenbank.
	 * @throws SQLException
	 */
	public static void init()
		throws SQLException
	{
		con = getConnection();
	}
	
	/**
	 * Führt den Commit der statischen Verbindung durch und schließt diese.
	 * @throws SQLException
	 */
	public static void commitAndClose()
		throws SQLException
	{
		con.commit();
		
		con.close();
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
	
    /**
     * Laufroutine des Lasten-Threads.
     */
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
	
	/**
	 * Führt die Phase mit gegebener Zeitspanne aus.
	 * @param phaseName Die Bezeichnung der auszuführenden Phase.
	 * @param timeInSec Die Zeitspanne der Phase in Sekunden.
	 * @throws SQLException
	 * @throws InterruptedException
	 */
	private void executePhase(String phaseName, int timeInSec)
		throws SQLException, InterruptedException
	{
		executePhase(phaseName, timeInSec, false);
	}
	
	/**
	 * Führt die Phase mit gegebener Zeitspanne aus.
	 * @param phaseName Die Bezeichnung der auszuführenden Phase.
	 * @param timeInSec Die Zeitspanne der Phase in Sekunden.
	 * @param setValues Gibt an, ob die Ergebnisse der Phase gespeichert werden.
	 * @throws SQLException
	 * @throws InterruptedException
	 */
	private void executePhase(String phaseName, int timeInSec, boolean setValues)
		throws SQLException, InterruptedException
	{
		System.out.println(phaseName + ": " + timeInSec + "s");
		
		int timeSpan = timeInSec * 1000;
		long timeStart = System.currentTimeMillis();
		
		initGlobalStatements();
		
		int count = 0;

		while ((System.currentTimeMillis() - timeStart) < timeSpan)
		{
			executeRandom();
			
			count++;
			
			Thread.sleep(50);
		}
		
		closeGlobalStatements();

		if (setValues)
		{
			anzahl = count;
			tps = (double)count / timeInSec;
		}
	}
	
	/**
	 * Führt eine zufällige Transaktion aus.
	 * @throws SQLException
	 */
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
	
	/**
	 * Initialisiert die globalen Statements.
	 * @throws SQLException
	 */
	private void initGlobalStatements()
		throws SQLException
	{
		prepStmtKontostand = con.prepareStatement("select balance from accounts where accid = ?");
		
		callStmtEinzahlung = con.prepareCall("{call einzahlung(?,?,?,?,?)}");
		
		prepStmtAnalyse = con.prepareStatement("select count(*) as anz from history where delta = ?");
	}
	
	/**
	 * Schliesst die globalen Statements.
	 * @throws SQLException
	 */
	private void closeGlobalStatements()
		throws SQLException
	{
		prepStmtKontostand.close();
		
		callStmtEinzahlung.close();
		
		prepStmtAnalyse.close();
	}
	
	/**
	 * Führt die Transaktion 'Kontostand' aus.
	 * Liest den Kontostand eines gegebenen Accounts aus.
	 * @param accid Die Id des Accounts.
	 * @return Gibt den Kontostand des Accounts zurück.
	 * @throws SQLException
	 */
	private int kontostand(int accid)
		throws SQLException
	{
		prepStmtKontostand.setInt(1, accid);
		
		ResultSet set = prepStmtKontostand.executeQuery();
		
		set.next();

		return set.getInt("balance");
	}
	
	/**
	 * Führt die Transaktion 'Einzahlung' aus.
	 * Zahlt einen gegebenen Betrag in ein Konto ein.
	 * @param accid Die Id des Accounts.
	 * @param tellerid Die Id des verwendeten Geldautomaten.
	 * @param branchid Die Id der Bank-Filiale.
	 * @param delta Der einzuzahlende Betrag.
	 * @return Gibt den aktualisierten Kontostand zurück.
	 * @throws SQLException
	 */
	private int einzahlung(int accid, int tellerid, int branchid, int delta)
		throws SQLException
	{
		callStmtEinzahlung.setInt(1, accid);
		callStmtEinzahlung.setInt(2, tellerid);
		callStmtEinzahlung.setInt(3, branchid);
		callStmtEinzahlung.setInt(4, delta);
		callStmtEinzahlung.registerOutParameter(5, java.sql.Types.INTEGER);
		
		callStmtEinzahlung.executeUpdate();
		
		return callStmtEinzahlung.getInt(5);
	}
	
	/**
	 * Führt die Transaktion 'Analyse' aus.
	 * Zählt die Historien-Einträge mit gegebenem Einzahlungswert.
	 * @param delta Der zu suchende Einzahlungswert.
	 * @return Gibt die Anzahl der Datensätze mit gegebenem Einzahlungswert zurück.
	 * @throws SQLException
	 */
	private int analyse(int delta)
		throws SQLException
	{
		prepStmtAnalyse.setInt(1, delta);
		
		ResultSet set = prepStmtAnalyse.executeQuery();
		
		set.next();
		
		return set.getInt("anz");
	}
	
}
