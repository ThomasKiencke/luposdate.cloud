package lupos.cloud.testing;

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
		pigServer = new PigServer(ExecType.MAPREDUCE);
		pigServer.debugOff();
		// pigServer.registerFunction("PigLoadUDF", new FuncSpec(
		// "lupos.cloud.pig.udfs.PigLoadUDF"));
		pigServer
				.registerQuery("po_s_data = load 'hbase://PO_S' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('VALUE', '-loadKey true') as (predicate_object:chararray, subject_map:map[]);");

		pigServer
				.registerQuery("p_so_data = load 'hbase://P_SO' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('VALUE', '-loadKey true') as (predicate:chararray, subject_object_map:map[]);");

		pigServer
				.registerQuery("PATTERN1 = FILTER po_s_data BY predicate_object == '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>,<http://localhost/vocabulary/bench/Journal>';");
		pigServer
				.registerQuery("PATTERN2 = FILTER po_s_data BY predicate_object == '<http://purl.org/dc/elements/1.1/title>,\"Journal 1 (1940)\"^^<http://www.w3.org/2001/XMLSchema#string>';");
		pigServer
				.registerQuery("PATTERN3 = FILTER p_so_data BY predicate == '<http://purl.org/dc/terms/issued>';");
		
		pigServer
				.registerQuery("BAG1 = foreach PATTERN1 generate flatten(lupos.cloud.pig.udfs.MapToBag(subject_map)) as (subject:chararray);");
		pigServer
				.registerQuery("BAG2 = foreach PATTERN2 generate flatten(lupos.cloud.pig.udfs.MapToBag(subject_map)) as (subject:chararray);");
		pigServer
		.registerQuery("BAG3 = foreach PATTERN3 generate flatten(lupos.cloud.pig.udfs.MapToBag(subject_object_map)) as (subject_object:chararray);");

		
		pigServer.registerQuery("X1 = JOIN BAG1 BY $0, BAG2 BY subject;");
		pigServer.registerQuery("X11 = FOREACH X1 GENERATE $0;");
		pigServer.registerQuery("X2 = JOIN X1 BY $0, BAG3 BY $0;");
		pigServer.registerQuery("X = FOREACH X2 GENERATE $1;");
		
		
		// System.out.println(pigServer.getExamples("p1").toString());

		// printAlias("PATTERN1");
		// Bag 1
		// System.out.println("Bag 1 -----");
		// printAlias("PATTERN1");
		// printAlias("BAG1");
		// System.out.println("Bag 2-------");
		// printAlias("BAG2");

		System.out.println("JOIN ------");
		printAlias("X");

	}

	public static void printAlias(String input) throws IOException {
		Iterator<Tuple> i = pigServer.openIterator(input);
		while (i.hasNext()) {
			Tuple t = i.next();
			String out = t.toString();
			System.out.println(out);
		}
	}
}