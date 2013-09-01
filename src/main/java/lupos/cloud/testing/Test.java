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
				.registerQuery("PATTERN_0 = load 'hbase://PO_S' using lupos.cloud.pig.udfs.HBaseLoadUDF('VALUE', '','<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>,<http://localhost/vocabulary/bench/Inproceedings>') as (columncontent_0:map[]);INTERMEDIATE_BAG_0 = foreach PATTERN_0 generate flatten(lupos.cloud.pig.udfs.MapToBagUDF($0)) as (output0:chararray);PATTERN_1 = load 'hbase://P_SO' using lupos.cloud.pig.udfs.HBaseLoadUDF('VALUE', '','<http://purl.org/dc/elements/1.1/creator>') as (columncontent_1:map[]);INTERMEDIATE_BAG_1 = foreach PATTERN_1 generate flatten(lupos.cloud.pig.udfs.MapToBagUDF($0)) as (output1_1:chararray, output2_1:chararray);PATTERN_2 = load 'hbase://P_SO' using lupos.cloud.pig.udfs.HBaseLoadUDF('VALUE', '','<http://localhost/vocabulary/bench/booktitle>') as (columncontent_2:map[]);INTERMEDIATE_BAG_2 = foreach PATTERN_2 generate flatten(lupos.cloud.pig.udfs.MapToBagUDF($0)) as (output1_2:chararray, output2_2:chararray); PATTERN_3 = load 'hbase://P_SO' using lupos.cloud.pig.udfs.HBaseLoadUDF('VALUE', '','<http://purl.org/dc/elements/1.1/title>') as (columncontent_3:map[]);INTERMEDIATE_BAG_3 = foreach PATTERN_3 generate flatten(lupos.cloud.pig.udfs.MapToBagUDF($0)) as (output1_3:chararray, output2_3:chararray); PATTERN_4 = load 'hbase://P_SO' using lupos.cloud.pig.udfs.HBaseLoadUDF('VALUE', '','<http://purl.org/dc/terms/partOf>') as (columncontent_4:map[]);INTERMEDIATE_BAG_4 = foreach PATTERN_4 generate flatten(lupos.cloud.pig.udfs.MapToBagUDF($0)) as (output1_4:chararray, output2_4:chararray); PATTERN_5 = load 'hbase://P_SO' using lupos.cloud.pig.udfs.HBaseLoadUDF('VALUE', '','<http://www.w3.org/2000/01/rdf-schema#seeAlso>') as (columncontent_5:map[]);INTERMEDIATE_BAG_5 = foreach PATTERN_5 generate flatten(lupos.cloud.pig.udfs.MapToBagUDF($0)) as (output1_5:chararray, output2_5:chararray); PATTERN_6 = load 'hbase://P_SO' using lupos.cloud.pig.udfs.HBaseLoadUDF('VALUE', '','<http://swrc.ontoware.org/ontology#pages>') as (columncontent_6:map[]);INTERMEDIATE_BAG_6 = foreach PATTERN_6 generate flatten(lupos.cloud.pig.udfs.MapToBagUDF($0)) as (output1_6:chararray, output2_6:chararray); PATTERN_7 = load 'hbase://P_SO' using lupos.cloud.pig.udfs.HBaseLoadUDF('VALUE', '','<http://xmlns.com/foaf/0.1/homepage>') as (columncontent_7:map[]);INTERMEDIATE_BAG_7 = foreach PATTERN_7 generate flatten(lupos.cloud.pig.udfs.MapToBagUDF($0)) as (output1_7:chararray, output2_7:chararray); PATTERN_8 = load 'hbase://P_SO' using lupos.cloud.pig.udfs.HBaseLoadUDF('VALUE', '','<http://purl.org/dc/terms/issued>') as (columncontent_8:map[]);INTERMEDIATE_BAG_8 = foreach PATTERN_8 generate flatten(lupos.cloud.pig.udfs.MapToBagUDF($0)) as (output1_8:chararray, output2_8:chararray); INTERMEDIATE_BAG_9 = JOIN INTERMEDIATE_BAG_0 BY $0, INTERMEDIATE_BAG_1 BY $0, INTERMEDIATE_BAG_2 BY $0, INTERMEDIATE_BAG_3 BY $0, INTERMEDIATE_BAG_4 BY $0, INTERMEDIATE_BAG_5 BY $0, INTERMEDIATE_BAG_6 BY $0, INTERMEDIATE_BAG_7 BY $0, INTERMEDIATE_BAG_8 BY $0;INTERMEDIATE_BAG_10 = FOREACH INTERMEDIATE_BAG_9 GENERATE $2, $0, $6, $10, $14, $4, $16, $8, $12;PATTERN_11 = load 'hbase://P_SO' using lupos.cloud.pig.udfs.HBaseLoadUDF('VALUE', '','<http://localhost/vocabulary/bench/abstract>') as (columncontent_0:map[]);INTERMEDIATE_BAG_11 = foreach PATTERN_11 generate flatten(lupos.cloud.pig.udfs.MapToBagUDF($0)) as (output1_0:chararray, output2_0:chararray);INTERMEDIATE_BAG_12 = JOIN INTERMEDIATE_BAG_10 BY $1 LEFT OUTER, INTERMEDIATE_BAG_11 BY $0;X = ORDER INTERMEDIATE_BAG_12 BY $6;");

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