import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Last extends Thread
{
	private static Connection con;
	
	private static final java.util.SplittableRandom random = new java.util.SplittableRandom();
	
	public int anzahl;
	public double tps;
	
	public static void buildConnection()
		throws SQLException
	{
		con = getConnection();
	}
	
	public static void commitConnection()
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

        return con;
    }
	
	@Override
	public void run()
	{
		try
		{
			long time = System.currentTimeMillis();
			
			while ((System.currentTimeMillis() - time) < 4000)
			{
				executeRandom();
				
				Thread.sleep(50);
			}
			
			time = System.currentTimeMillis();
			
			int count = 0;
			
			while ((System.currentTimeMillis() - time) < 5000)
			{
				executeRandom();
				
				count++;
				
				Thread.sleep(50);
			}
	
			time = System.currentTimeMillis();
			
			while ((System.currentTimeMillis() - time) < 1000)
			{
				executeRandom();
				
				Thread.sleep(50);
			}
			
			anzahl = count;
			System.out.println("Anzahl: " + count);
			tps = (double)count / 5;
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
	
	private static void executeRandom()
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
	
	private static int kontostand(int accid)
		throws SQLException
	{
		Statement stmt = con.createStatement();
		
		ResultSet set = stmt.executeQuery("select balance from accounts where accid = " + accid);
		
		set.next();

		return set.getInt("balance");
	}
	
	private static int einzahlung(int accid, int tellerid, int branchid, int delta)
		throws SQLException
	{
		Statement stmt = con.createStatement();
		
		stmt.execute("update accounts set balance = balance + " + delta + " where accid = " + accid);
		int accbalance = kontostand(accid);
		stmt.execute("insert into history (accid, tellerid, delta, branchid, accbalance, cmmnt)"
				+ " values (" + accid + ", " + tellerid + ", " + delta + ", " + branchid + ", " + accbalance + ", '')");
		stmt.execute("update tellers set balance = balance + " + delta + " where tellerid = " + tellerid);
		stmt.execute("update branches set balance = balance + " + delta + " where branchid = " + branchid);

		return accbalance;
	}
	
	private static int analyse(int delta)
		throws SQLException
	{
		Statement stmt = con.createStatement();
		
		ResultSet set = stmt.executeQuery("select count(*) as anz from history where delta = " + delta);
		
		set.next();
		
		return set.getInt("anz");
	}
	
}
