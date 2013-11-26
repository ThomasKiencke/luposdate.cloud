package lupos.cloud.applications;

import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;

import org.apache.pig.impl.io.FileLocalizer;

import lupos.cloud.query.CloudEvaluator;
import lupos.cloud.storage.util.CloudManagement;
import lupos.datastructures.bindings.Bindings;
import lupos.datastructures.bindings.BindingsMap;
import lupos.datastructures.items.literal.LiteralFactory;
import lupos.datastructures.items.literal.URILiteral;
import lupos.datastructures.items.literal.LiteralFactory.MapType;
import lupos.datastructures.queryresult.QueryResult;
import lupos.engine.evaluators.BasicIndexQueryEvaluator;

/**
 * Dieser Executer führt beliebige SPARQL-Anfragen aus.
 */
public class QueryExecuter {

	private static CloudEvaluator cloudEvaluator;
	private static double[] result_time;
	private static double[] result_queryresult;
	private static double[] result_bitvectorTime;
	private static boolean printResults = true;
	static SimpleDateFormat formatter = new SimpleDateFormat(
			"yyyy.MM.dd HH:mm:ss");

	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			System.exit(0);
		}

		CloudManagement.PARALLEL_REDUCE_OPERATIONS = Integer.parseInt(args[0]);

		String testOnly = args[1];
		boolean printResults = true;

		if (args[2].equals("nosize")) {
			printResults = false;
		}

		cloudEvaluator = new CloudEvaluator();

		Bindings.instanceClass = BindingsMap.class;

		LiteralFactory.setType(MapType.NOCODEMAP);

		QueryResult.type = QueryResult.TYPE.ADAPTIVE;

		result_time = new double[args.length - 3];
		result_queryresult = new double[args.length - 3];
		result_bitvectorTime = new double[args.length - 3];

		// Tests ausführen:
		try {
			if (testOnly.equals("both") || testOnly.equals("first")) {
				System.out.println(formatter.format(new Date()).toString()
						+ ": Tests werden ausgefuehrt (mit bloomfilter):");

				for (int i = 0; i < args.length - 3; i++) {
					testQuery(i, args[i + 3], printResults);
					FileLocalizer.deleteTempFiles(); // loescht temp files auf
														// HDFS
				}
				printCSV();
			}

			if (testOnly.equals("both") || testOnly.equals("second")) {

				cloudEvaluator.getCloudManagement().bitvectorTime = 0.0;
				cloudEvaluator.getCloudManagement().bloomfilter_active = false;
				System.out.println(formatter.format(new Date()).toString()
						+ ": Tests werden ausgefuehrt (ohne bloomfilter):");
				for (int i = 0; i < args.length - 3; i++) {
					testQuery(i, args[i + 3], printResults);
					FileLocalizer.deleteTempFiles(); // loescht temp files auf
														// HDFS
				}
				printCSV();
			}

			cloudEvaluator.shutdown(); // gibt temporäre dateien frei
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void printCSV() {
		System.out.println("CSV Format: ");
		int i = 0;
		for (double result : result_time) {
			System.out.println(result + "\t" + result_bitvectorTime[i] + "\t"
					+ result_queryresult[i]);
			i++;
		}
	}

	protected static String readFile(String file) {
		InputStream is = QueryExecuter.class.getClassLoader()
				.getResourceAsStream(file);
		String selectQuery = convertStreamToString(is);
		return selectQuery;
	}

	private static String convertStreamToString(java.io.InputStream is) {
		java.util.Scanner s = new java.util.Scanner(is).useDelimiter("\\A");
		return s.hasNext() ? s.next() : "";
	}

	protected static QueryResult executeQuery(
			BasicIndexQueryEvaluator evaluator, String query) throws Exception {
		evaluator.prepareInputData(new LinkedList<URILiteral>(),
				new LinkedList<URILiteral>()); // / workaround weil sonst
												// nullpointerexception
		evaluator.compileQuery(query);
		evaluator.logicalOptimization();
		evaluator.physicalOptimization();

		return evaluator.getResult(true);
	}

	public static void testQuery(int number, String filename, boolean printSize)
			throws Exception {
		System.out.println("\nTest " + number + " Input: " + filename);
		long start = System.currentTimeMillis();
		String selectQuery = readFile(filename);
		QueryResult actual = executeQuery(cloudEvaluator, selectQuery);
		long stop = System.currentTimeMillis();
		result_time[number] = (double) ((stop - start) / (double) 1000);
		System.out.print("Time: " + (double) ((stop - start) / (double) 1000)
				+ " Sekunden");
		if (printResults) {
			int resultSize = -1;
			if (printSize) {
				resultSize = actual.oneTimeSize();
			}
			result_queryresult[number] = resultSize;
			System.out.print("- Results: " + resultSize);
		}
		System.out.println();
		result_bitvectorTime[number] = cloudEvaluator.getCloudManagement()
				.getBitvectorTime();
	}
}
