package lupos.cloud.pig.udfs;

import java.io.IOException;

import lupos.cloud.hbase.HBaseConnection;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.pig.EvalFunc;
import org.apache.pig.LoadFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class TestLoadUDF extends EvalFunc<DataBag> {

	@Override
	public DataBag exec(Tuple input) throws IOException {
		String tablename = input.get(0).toString();
		String rowKey = input.get(1).toString();
		HBaseConnection.init();
		HTable table = new HTable(HBaseConnection.getConfiguration(), tablename);

		Scan scan = new Scan();
		if (rowKey != null) {
			scan.setStartRow(Bytes.toBytes(rowKey));
			// add random string because stopRow is exclusiv
			scan.setStopRow(Bytes.toBytes(rowKey + "z"));
		}
		scan.setCacheBlocks(false);
		ResultScanner scanner = table.getScanner(scan);
		DataBag result = BagFactory.getInstance().newDefaultBag();
	

		return null;
	}

}
