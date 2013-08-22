package lupos.cloud.testing;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

import lupos.cloud.hbase.HBaseConnection;
import lupos.misc.FileHelper;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigServer;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class Test2 {
	static PigServer pigServer = null;

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
//		HBaseConnection.init();
		
		String tablename = "P_SO";
		String rowKey = "<http://purl.org/dc/terms/issued>";
		HBaseConnection.init();
		HTable table = new HTable(HBaseConnection.getConfiguration(), tablename);

		String cf = "VALUE";
		String column = "<http://localhost/publications/journals/Journal1/1940>";
		
		Get g = new Get(Bytes.toBytes(rowKey));
		g.addFamily(cf.getBytes());
		g.setFilter(new ColumnPrefixFilter(column.getBytes()));
		
		Result result = table.get(g);

		System.out.println(result.toString());
//		Scan scan = new Scan();
//		if (rowKey != null) {
//			scan.setStartRow(Bytes.toBytes(rowKey));
//			// add random string because stopRow is exclusiv
//			scan.setStopRow(Bytes.toBytes(rowKey + "z"));
//		}
//		scan.setCacheBlocks(false);
//		ResultScanner scanner = table.getScanner(scan);
//		DataBag result = BagFactory.getInstance().newDefaultBag();
//		
//		Result result2 = scanner.next();

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