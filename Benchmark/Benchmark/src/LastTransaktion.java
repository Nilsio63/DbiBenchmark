import java.sql.SQLException;

/**
 * Programm zum Messen des Transaktionsdurchsatzes einer Benchmark-Datenbank
 * über 5 parallel laufende Lasten.
 * @author Leon Arndt, Nils Balke, Alina Wewering
 * @version 5.0
 */
public class LastTransaktion
{
	public static void main(String[] args)
	{
		try
		{
			Last.init();
			
			Last last1 = new Last();
			Last last2 = new Last();
			Last last3 = new Last();
			Last last4 = new Last();
			Last last5 = new Last();

			last1.start();
			last2.start();
			last3.start();
			last4.start();
			last5.start();
			
			last1.join();
			last2.join();
			last3.join();
			last4.join();
			last5.join();
			
			int anzahlGesamt = last1.anzahl
					+ last2.anzahl
					+ last3.anzahl
					+ last4.anzahl
					+ last5.anzahl;
			
			double tpsGesamt = last1.tps
					+ last2.tps
					+ last3.tps
					+ last4.tps
					+ last5.tps;
			
			double tpsSchnitt = tpsGesamt / 5;
			
			System.out.println();
			System.out.println("Anzahl Gesamt: " + anzahlGesamt);
			System.out.println("TPS Gesamt: " + tpsGesamt);
			System.out.println("TPS Durchschnitt: " + tpsSchnitt);
			
			Last.commitAndClose();
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

}
