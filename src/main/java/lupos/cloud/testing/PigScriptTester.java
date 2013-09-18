package lupos.cloud.testing;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import lupos.cloud.gui.QueryExecuter;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;

public class PigScriptTester {
	static PigServer pigServer = null;
	static long stopMiddle;

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		
		if(args.length != 1) {
			System.out.println("java -jar programm <pig script>");
			System.exit(0);
		}
		long start = System.currentTimeMillis();

		pigServer = new PigServer(ExecType.MAPREDUCE);
//		pigServer.getPigContext().getConf().setProperty("hbase.zookeeper.quorum", "pc08:2181");
		
		String pigInputProgramm = readFile(args[0]);
		pigServer.registerQuery(pigInputProgramm);
		printAlias("X"); // own function, which itereate over the keys

		long stop = System.currentTimeMillis();
		System.out.println("Dauer ohne Uebertragung: " + ((stopMiddle - start) / 1000) + "s");
		System.out.println("Dauer mit Uebertragung: " + ((stop - start) / 1000) + "s");
		
		pigServer.shutdown();
		

	}

	public static void printAlias(String input) throws IOException {
		String merken = "";
		int merki = 0;
		Iterator<Tuple> t = pigServer.openIterator(input);
		int i = 0;
		stopMiddle = System.currentTimeMillis();
		while (t.hasNext()) {
			Tuple tupel = t.next();
			i++;
//			String out = tupel.toString();
			// if (merki < out.length())
			// merken = out;
//			System.out.println(out);
		}
		System.out.println("Anzahl: " + i);
		// System.out.println(merken);
	}
	
	protected static String readFile(String file) {
		InputStream is = PigScriptTester.class.getClassLoader()
				.getResourceAsStream(file);
		String selectQuery = convertStreamToString(is);
		return selectQuery;
	}

	private static String convertStreamToString(java.io.InputStream is) {
		java.util.Scanner s = new java.util.Scanner(is).useDelimiter("\\A");
		return s.hasNext() ? s.next() : "";
	}
}