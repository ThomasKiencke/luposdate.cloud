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

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
//		Properties props = new Properties();
//		props.setProperty("fs.default.name", "hdfs://192.168.2.45:8020");
//		props.setProperty("mapred.job.tracker", "192.168.2.45:8021");
//		PigServer pigServer = new PigServer(ExecType.MAPREDUCE, props);
		PigServer pigServer = new PigServer(ExecType.MAPREDUCE);
		pigServer.registerFunction("PigLoadUDF", new FuncSpec(
				"lupos.cloud.pig.udfs.PigLoadUDF"));
//		pigServer
//				.registerQuery("p1 = load 'hbase://S_PO' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('VALUE', '-loadKey true') as (rowkey:bytearray, VALUE:map[]);");
		pigServer.registerQuery("data = load 'hbase://test_table' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('stats:count', '-loadKey true') AS (id:chararray, count:chararray);");
		Iterator i = pigServer.openIterator("data");

		while (i.hasNext()) {
			// org.apache.pig.data.Tuple t = (org.apache.pig.data.Tuple)
			i.next();
			Tuple t = (Tuple) i.next();
			System.out.println(t.get(1));
		}

	}
}