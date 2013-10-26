package lupos.cloud.applications;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URISyntaxException;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;

import lupos.datastructures.items.Triple;
import lupos.rdf.parser.NquadsParser;

/**
 * Quad -> Triple
 */
public class QuadToN3Converter {

	/**
	 * Main Methode.
	 * 
	 * @param args
	 *            the arguments
	 * @throws Exception
	 *             the exception
	 */
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Parameter: <quad_input_file> <quad_output_file>");
			System.exit(0);
		}
		
		InputStream is = QuadToN3Converter.class.getClassLoader()
				.getResourceAsStream(args[0]);
		PrintWriter writer = new PrintWriter(args[1], "UTF-8");

		final NxParser nxp = new NxParser(is);
		
		
		long startTime = System.currentTimeMillis();
		int number = 0;
		while (nxp.hasNext()) {
			final Node[] ns = nxp.next();
			number++;
			if (number % 1000000 == 0){
				System.out.println("#triples:" + number);
			}
			try {
				writer.println(new Triple(NquadsParser.transformToLiteral(ns[0]),
						NquadsParser.transformToLiteral(ns[1]),
						NquadsParser.transformToLiteral(ns[2])).toN3String());
			} catch (URISyntaxException e) {
				System.err.println(e);
				e.printStackTrace();
			}
		}

		writer.close();
		long stopTime = System.currentTimeMillis();
		System.out.println("Generierte Tripel " + number
				+ " Dauer: " + (stopTime - startTime) / 1000 + "s");
	}
}
