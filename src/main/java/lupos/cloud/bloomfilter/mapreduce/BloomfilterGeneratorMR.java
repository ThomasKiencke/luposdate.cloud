package lupos.cloud.bloomfilter.mapreduce;

import java.io.IOException;
import java.util.ArrayList;

import lupos.cloud.hbase.HBaseConnection;
import lupos.cloud.hbase.HBaseDistributionStrategy;

/**
 * * Mit Hilfe dieser Klasse wird die Byte-Bitvektorgeneierung gestartet. Die
 * Ausführung erfolgt über mehrere MapReduce-Jobs (für jede Tabelle einer).
 */
public class BloomfilterGeneratorMR {

	/**
	 * Fuer jeden Bitvektor mit einer Kardinalität von > MIN_CARD wird der
	 * Byte-Bitvektor erzeugt.
	 */
	public static Integer MIN_CARD = 25000;

	/**
	 * Beschreibt die maximale Anzahl der Key-Value Paare die pro
	 * scan.next()-Aufruf übertragen wird. Diese Zahl darf nicht zu groß sein,
	 * denn HBase lädt das gesamte Ergebnis in den Arbeitsspeicher.
	 */
	public static Integer BATCH = 5000;

	/** The caching. */
	public static Integer CACHING = 100;

	/**
	 * The main method.
	 * 
	 * @param args
	 *            the arguments
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 * @throws InterruptedException
	 *             the interrupted exception
	 * @throws ClassNotFoundException
	 *             the class not found exception
	 */
	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		System.out.println("Starts with b: " + BATCH + " c: " + CACHING);
		HBaseConnection.init();
		ArrayList<BVJobThread> jobList = new ArrayList<BVJobThread>();

		long startTime = System.currentTimeMillis();

		String[] tables = HBaseDistributionStrategy.getTableInstance()
				.getTableNames();


		for (String tablename : tables) {
			System.out.println("Aktuelle Tabelle: " + tablename);
			BVJobThread curJob = new BVJobThread(tablename);
			jobList.add(curJob);
			curJob.start();

		}

		System.out.println("Warte bis alle Jobs abgeschlossen sind ...");
		for (BVJobThread job : jobList) {
			while (!job.isFinished()) {
				sleep(2000);
			}
			System.out.println(job.getTablename() + " ist fertig");
		}

		long stopTime = System.currentTimeMillis();
		System.out.println("Bitvektor Generierung beendet." + " Dauer: "
				+ (stopTime - startTime) / 1000 + "s");
	}

	/**
	 * Sleep.
	 * 
	 * @param time
	 *            the time
	 */
	private static void sleep(int time) {
		try {
			Thread.sleep(time);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
