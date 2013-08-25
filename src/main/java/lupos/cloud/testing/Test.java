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
//		pigServer
//				.registerQuery("PATTERN_0 = load 'hbase://PO_S' using lupos.cloud.pig.udfs.PigLoadUDF('VALUE', '-loadKey true','<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>,<http://localhost/vocabulary/bench/Journal>') as (rowkey:chararray, columncontent:map[]);");
//		pigServer
//				.registerQuery("INTERMEDIATE_BAG_0 = foreach PATTERN_0 generate flatten(lupos.cloud.pig.udfs.MapToBag($1)) as (output:chararray);");
//		pigServer
//				.registerQuery("X = load 'hbase://P_SO' using lupos.cloud.pig.udfs.PigLoadInformationPassingUDF('VALUE', '-loadKey true', columncontent) as (rowkey:chararray, columncontent:map[]);");
		pigServer.registerQuery("S_PO_DATA = load 'hbase://S_PO' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('VALUE', '-loadKey true') as (rowkey:chararray, columncontent:map[]);INTERMEDIATE_BAG_0 = foreach S_PO_DATA generate $0, flatten(lupos.cloud.pig.udfs.MapToBagUDF($1));PATTERN_1 = load 'hbase://S_PO' using lupos.cloud.pig.udfs.HBaseLoadUDF('VALUE', '','<http://example/book3>') as (columncontent:map[]);INTERMEDIATE_BAG_1 = foreach PATTERN_1 generate flatten(lupos.cloud.pig.udfs.MapToBagUDF($0)) as (output1:chararray, output2:chararray); INTERMEDIATE_BAG_2 = JOIN INTERMEDIATE_BAG_0 BY ($1,$2), INTERMEDIATE_BAG_1 BY ($0,$1);X = FOREACH INTERMEDIATE_BAG_2 GENERATE $0, $1, $2;");
		pigServer.registerQuery("S_PO_DATA = load 'hbase://S_PO' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('VALUE', '-loadKey true') as (rowkey:chararray, columncontent:map[]);INTERMEDIATE_BAG_0 = foreach S_PO_DATA generate $0, flatten(lupos.cloud.pig.udfs.MapToBagUDF($1));PATTERN_1 = load 'hbase://S_PO' using lupos.cloud.pig.udfs.HBaseLoadUDF('VALUE', '','<http://example/book3>') as (columncontent:map[]);INTERMEDIATE_BAG_1 = foreach PATTERN_1 generate flatten(lupos.cloud.pig.udfs.MapToBagUDF($0)) as (output1:chararray, output2:chararray); INTERMEDIATE_BAG_2 = JOIN INTERMEDIATE_BAG_1 BY ($0,1), INTERMEDIATE_BAG_0 BY ($1,2);X = FOREACH INTERMEDIATE_BAG_2 GENERATE $0, $1, $2;");
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