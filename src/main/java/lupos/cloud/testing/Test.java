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
		// Properties props = new Properties();
		// props.setProperty("fs.default.name", "hdfs://192.168.2.45:8020");
		// props.setProperty("mapred.job.tracker", "192.168.2.45:8021");
		// PigServer pigServer = new PigServer(ExecType.MAPREDUCE, props);

		// PigServer pigServer = new PigServer(ExecType.MAPREDUCE);
		// Properties props = new Properties();
		// props.setProperty("fs.defaultFS", "hdfs://192.168.2.22:8020");
		// props.setProperty("mapred.job.tracker", "192.168.2.22:8021");
		// PigServer pigServer = new PigServer(ExecType.MAPREDUCE, props);
		// pigServer = new PigServer(ExecType.LOCAL);
		// pigServer.debugOff();
		// long start = System.currentTimeMillis();
		// pigServer
		// .registerQuery("PO_S_DATA = load 'hbase://PO_S' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('VALUE', '-loadKey true -noWAL') as (rowkey:chararray, columncontent:map[]);PATTERN_0 = FILTER PO_S_DATA BY $0 == '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>,<http://localhost/vocabulary/bench/Journal>';INTERMEDIATE_BAG_0 = foreach PATTERN_0 generate flatten(lupos.cloud.pig.udfs.MapToBag($1)) as (output:chararray);PO_S_DATA = load 'hbase://PO_S' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('VALUE', '-loadKey true -noWAL') as (rowkey:chararray, columncontent:map[]);PATTERN_1 = FILTER PO_S_DATA BY $0 == '<http://purl.org/dc/elements/1.1/title>,\"Journal 1 (1940)\"^^<http://www.w3.org/2001/XMLSchema#string>';INTERMEDIATE_BAG_1 = foreach PATTERN_1 generate flatten(lupos.cloud.pig.udfs.MapToBag($1)) as (output:chararray);P_SO_DATA = load 'hbase://P_SO' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('VALUE', '-loadKey true -noWAL') as (rowkey:chararray, columncontent:map[]);PATTERN_2 = FILTER P_SO_DATA BY $0 == '<http://purl.org/dc/terms/issued>';INTERMEDIATE_BAG_2 = foreach PATTERN_2 generate flatten(lupos.cloud.pig.udfs.MapToBag($1)) as (output1:chararray, output2:chararray);A = ORDER INTERMEDIATE_BAG_0 BY $0; B = ORDER INTERMEDIATE_BAG_1 BY $0; INTERMEDIATE_BAG_3 = JOIN A BY $0, B BY $0; X = FOREACH INTERMEDIATE_BAG_3 GENERATE $0;");

		// pigServer
		// .registerQuery("PO_S_DATA = load 'hbase://PO_S' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('VALUE:*', '-loadKey true -minTimestamp 1000'); PATTERN_0 = FILTER PO_S_DATA BY $0 == '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>,<http://localhost/vocabulary/bench/Journal>';");
		// System.out.println("JOIN ------");
		// printAlias("PATTERN_0");
		// org.apache.pig.backend.hadoop.hbase.HBaseStorage a;
		// pigServer.registerQuery("PATTERN_0 = load 'hbase://PO_S' using lupos.cloud.pig.udfs.PigLoadUDF('VALUE', '-loadKey true','<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>,<http://localhost/vocabulary/bench/Article>') as (rowkey:chararray, columncontent:map[]);");
		// pigServer.registerQuery("PATTERN_1 = load 'hbase://P_SO' using lupos.cloud.pig.udfs.PigLoadUDF('VALUE', '-loadKey true','<http://swrc.ontoware.org/ontology#pages>') as (rowkey:chararray, columncontent:map[]);");
		// printAlias("PATTERN_1");
		// long stop = System.currentTimeMillis();

		// System.out.println("dauer: " + ((stop - start) / 1000) + "s");
		new File("bulkLoadDirectory/SO_P_rowBufferHFile.csv").delete();
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