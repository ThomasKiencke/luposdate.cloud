package lupos.cloud.gui;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.LinkedList;

import org.apache.hadoop.hbase.thrift.generated.Hbase;

import lupos.cloud.hbase.HBaseConnection;
import lupos.cloud.query.CloudEvaluator;
import lupos.datastructures.items.literal.LiteralFactory;
import lupos.datastructures.items.literal.URILiteral;

/**
 * Mit dieser Klasse und einen angegebenen Paramater ist es möglich effizient,
 * ohne den Umweg über die GUI, Tripel in HBase zu laden. Man hat die Wahl
 * zwischen zwei Modi wie die Daten in HBase geladen werden. Einmal per HBase
 * API und einmal per MapReduce Job. Die Map Reduce Job Variante bietet sich
 * dann an wenn man eine größere Menge an Tripel laden will.
 */
public class HBaseLoader {

	/**
	 * Main Methode.
	 * 
	 * @param args
	 *            the arguments
	 * @throws Exception
	 *             the exception
	 */
	public static void main(String[] args) throws Exception {
		if (args.length != 4) {
			System.out.println("Parameter: <n3 Pfad> <1/2 für normal oder BulkLoad> <HTriple Cache Size>");
			System.exit(0);
		}
		
		// init
		CloudEvaluator evaluator = new CloudEvaluator();
		if(args[1].equals("1")) {
			HBaseConnection.MAP_REDUCE_BULK_LOAD = false;
		} else {
			HBaseConnection.MAP_REDUCE_BULK_LOAD = true;
		}
		
		HBaseConnection.ROW_BUFFER_SIZE = Integer.parseInt(args[3]);
		HBaseConnection.deleteTableOnCreation = true;
		HBaseConnection.init();
		
		String file_path = args[0];
		FileReader fr = new FileReader(file_path);
		BufferedReader br = new BufferedReader(fr);

		StringBuilder prefix = new StringBuilder();
		String curLine = br.readLine();
		
		// Prefix merken
		while (curLine != null && curLine.startsWith("@")) {
			prefix.append(curLine);
			curLine = br.readLine();
		}
		
		long startTime = System.currentTimeMillis();
		long tripleAnzahl = 0;
		boolean run = true;
		
		// Tripel in 1000 Blöcke einlesen und den Evaluator übergeben
		while (run) {
			StringBuilder inputCache = new StringBuilder();

			for (int i = 0; i < 1000 && curLine != null; i++) {
				tripleAnzahl++;
				inputCache.append(curLine);
				curLine = br.readLine();
			}

			final URILiteral rdfURL = LiteralFactory
					.createStringURILiteral("<inlinedata:" + prefix.toString()
							+ "\n" + inputCache.toString() + ">");
			LinkedList<URILiteral> defaultGraphs = new LinkedList<URILiteral>();
			defaultGraphs.add(rdfURL);

			evaluator.prepareInputData(defaultGraphs,
					new LinkedList<URILiteral>());
			
			if (curLine == null) {
				run = false;
				break;
			}

		}
		br.close();

		HBaseConnection.flush();
		HBaseConnection.MAP_REDUCE_BULK_LOAD = false;
		HBaseConnection.deleteTableOnCreation = false;
		long stopTime = System.currentTimeMillis();
		System.out.println("Import ist beendet Triple: " + tripleAnzahl
				+ " Dauer: " + (stopTime - startTime) / 1000 + "s");
	}
}
