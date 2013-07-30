package lupos.cloud.testing;

import java.io.IOException;
import java.util.Iterator;


import org.apache.pig.FuncSpec;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;

public class Test {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		PigServer pigServer = new PigServer("local");
		pigServer.registerFunction("PigLoadUDF", new FuncSpec("lupos.cloud.pig.udfs.PigLoadUDF"));
		pigServer
				.registerQuery("p1 = load 'hbase://S_PO' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('VALUE', '-loadKey true') as (rowkey:bytearray, VALUE:map[]);");
		Iterator i = pigServer.openIterator("p1");
	
		
		while (i.hasNext()) {
//			org.apache.pig.data.Tuple t = (org.apache.pig.data.Tuple) i.next();
			Tuple t = (Tuple) i.next();
			System.out.println(t.get(1));
		}
	}
}
