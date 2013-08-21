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
				.registerQuery("PATTERN_0 = load 'hbase://PO_S' using lupos.cloud.pig.udfs.PigLoadUDF('VALUE', '-loadKey true','<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>,<http://localhost/vocabulary/bench/Journal>') as (rowkey:chararray, columncontent:map[]);");
//		pigServer
//				.registerQuery("INTERMEDIATE_BAG_0 = foreach PATTERN_0 generate flatten(lupos.cloud.pig.udfs.MapToBag($1)) as (output:chararray);");
		pigServer
				.registerQuery("X = load 'hbase://P_SO' using lupos.cloud.pig.udfs.PigLoadInformationPassingUDF('VALUE', '-loadKey true', columncontent) as (rowkey:chararray, columncontent:map[]);");

		printAlias("INTERMEDIATE_BAG_0");
		
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