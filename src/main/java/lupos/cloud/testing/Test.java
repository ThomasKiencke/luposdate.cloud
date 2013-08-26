package lupos.cloud.testing;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;

public class Test {
	static PigServer pigServer = null;

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		long start = System.currentTimeMillis();

		pigServer = new PigServer(ExecType.MAPREDUCE);

		pigServer
				.registerQuery("PATTERN_4 = load 'hbase://PO_S' using lupos.cloud.pig.udfs.HBaseLoadUDF('VALUE', '','<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>,<http://localhost/vocabulary/bench/Article>') as (columncontent:map[]);INTERMEDIATE_BAG_4 = foreach PATTERN_4 generate flatten(lupos.cloud.pig.udfs.MapToBagUDF($0)) as (output:chararray); PATTERN_5 = load 'hbase://P_SO' using lupos.cloud.pig.udfs.HBaseLoadUDF('VALUE', '','<http://swrc.ontoware.org/ontology#month>') as (columncontent:map[]);INTERMEDIATE_BAG_5 = foreach PATTERN_5 generate flatten(lupos.cloud.pig.udfs.MapToBagUDF($0)) as (output1:chararray, output2:chararray); INTERMEDIATE_BAG_6 = JOIN INTERMEDIATE_BAG_4 BY $0, INTERMEDIATE_BAG_5 BY $0;X = FOREACH INTERMEDIATE_BAG_6 GENERATE $0;");

		printAlias("X");

		long stop = System.currentTimeMillis();

		System.out.println("dauer: " + ((stop - start) / 1000) + "s");

	}

	public static void printAlias(String input) throws IOException {
		String merken = "";
		int merki = 0;
		Iterator<Tuple> i = pigServer.openIterator(input);
		while (i.hasNext()) {
			Tuple t = i.next();
			String out = t.toString();
			// if (merki < out.length())
			// merken = out;
			System.out.println(out);
		}
		// System.out.println(merken);
	}
}