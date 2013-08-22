package lupos.cloud.testing;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

import lupos.misc.FileHelper;

import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;

public class Test3 {
	static PigServer pigServer = null;

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		long start = System.currentTimeMillis();

		 pigServer = new PigServer(ExecType.MAPREDUCE);
		// pigServer
		// .registerQuery("PATTERN_0 = load 'hbase://PO_S' using lupos.cloud.pig.udfs.PigLoadUDF('VALUE', '','<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>,<http://localhost/vocabulary/bench/Journal>') as (columncontent:map[]); INTERMEDIATE_BAG_0 = foreach PATTERN_0 generate flatten(lupos.cloud.pig.udfs.MapToBag($0)) as (output:chararray);");
		// pigServer
		// .registerQuery("PATTERN_1 = foreach INTERMEDIATE_BAG_0 generate flatten(lupos.cloud.pig.udfs.LoadJoinUDF('false', 'VALUE', 'PO_S', $0));");
		// pigServer
		// .registerQuery("INTERMEDIATE_BAG_1 = foreach INTERMEDIATE_BAG_0 generate flatten(lupos.cloud.pig.udfs.LoadJoinUDF('VALUE','S_PO',$0));");
		// pigServer
		// .registerQuery("X = foreach INTERMEDIATE_BAG_1 generate flatten(lupos.cloud.pig.udfs.MapToBag($0)) as (output:chararray);");

		pigServer
				.registerQuery("PATTERN_0 = load 'hbase://PO_S' using lupos.cloud.pig.udfs.PigLoadUDF('VALUE', '','<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>,<http://localhost/vocabulary/bench/Journal>') as (columncontent:map[]);INTERMEDIATE_BAG_0 = foreach PATTERN_0 generate flatten(lupos.cloud.pig.udfs.MapToBag($0)) as (output:chararray);PATTERN_1 = foreach INTERMEDIATE_BAG_0 generate flatten(lupos.cloud.pig.udfs.LoadJoinUDF('false', 'VALUE', 'PO_S', '<http://purl.org/dc/elements/1.1/title>,\"Journal 1 (1940)\"^^<http://www.w3.org/2001/XMLSchema#string>', $0));INTERMEDIATE_BAG_1 = foreach PATTERN_1 generate flatten(lupos.cloud.pig.udfs.MapToBag($0)) as (output:chararray);");
		printAlias("INTERMEDIATE_BAG_1");

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