import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class LastTransaktion
{
	private static Connection con;
	
	public static void main(String[] args)
	{
		try
		{
			con = getConnection();
			
			System.out.print("kontostand(1): ");
			System.out.println(kontostand(1));
			System.out.print("einzahlung(1, 1, 1, 10): ");
			System.out.println(einzahlung(1, 1, 1, 10));
			System.out.print("analyse(10): ");
			System.out.println(analyse(10));
			
			con.commit();
		}
		catch (SQLException ex)
		{
			System.out.println("SQLException: " + ex.getMessage());
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
		
		stmt.execute("update branches set balance = balance + " + delta + " where branchid = " + branchid);
		stmt.execute("update tellers set balance = balance + " + delta + " where tellerid = " + tellerid);
		stmt.execute("update accounts set balance = balance + " + delta + " where accid = " + accid);
		int accbalance = kontostand(accid);
		stmt.execute("insert into history (accid, tellerid, delta, branchid, accbalance, cmmnt)"
				+ " values (" + accid + ", " + tellerid + ", " + delta + ", " + branchid + ", " + accbalance + ", '')");

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

}
