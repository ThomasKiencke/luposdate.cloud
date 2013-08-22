package lupos.cloud.gui;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.LinkedList;

import lupos.cloud.hbase.HBaseConnection;
import lupos.cloud.query.withsubgraphsubmission.CloudEvaluator;
import lupos.datastructures.items.literal.LiteralFactory;
import lupos.datastructures.items.literal.URILiteral;

public class HBaseLoader {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			System.out.println("Parameter: <n3 Pfad>");
			System.exit(0);
		}
		CloudEvaluator evaluator = new CloudEvaluator();
		String file_path = args[0];

		FileReader fr = new FileReader(file_path);
		BufferedReader br = new BufferedReader(fr);

		StringBuilder prefix = new StringBuilder();
		String curLine = br.readLine();
		while (curLine != null && curLine.startsWith("@")) {
			prefix.append(curLine);
			curLine = br.readLine();
		}
		long startTime = System.currentTimeMillis();
		long tripleAnzahl = 0;
		boolean run = true;
		while (run) {
			StringBuilder inputCache = new StringBuilder();
			curLine = br.readLine();

			for (int i = 0; i < 1000 && curLine != null; i++) {
				tripleAnzahl++;
				inputCache.append(curLine);
				curLine = br.readLine();
			}

			if (curLine == null) {
				run = false;
				break;
			}
			final URILiteral rdfURL = LiteralFactory
					.createStringURILiteral("<inlinedata:" + prefix.toString()
							+ "\n" + inputCache.toString() + ">");
			LinkedList<URILiteral> defaultGraphs = new LinkedList<URILiteral>();
			defaultGraphs.add(rdfURL);

			evaluator.prepareInputData(defaultGraphs,
					new LinkedList<URILiteral>());

		}
		br.close();
		
		HBaseConnection.flush();
		long stopTime = System.currentTimeMillis();
		System.out.println("Import ist beendet Triple: " + tripleAnzahl + " Dauer: " + (stopTime - startTime) / 1000 + "s");
	}
}
