package lupos.cloud.hbase.bulkLoad;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import au.com.bytecode.opencsv.CSVParser;

public class HBaseKVMapper extends
		Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
	CSVParser csvParser = new CSVParser('\t');
	String tableName = "";

	// ImmutableBytesWritable hKey = new ImmutableBytesWritable();
	// KeyValue kv;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration c = context.getConfiguration();
		tableName = c.get("hbase.table.name");
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] fields = null; //{"a", "b", "c", "d"};

		try {
			fields = csvParser.parseLine(value.toString());
		} catch (Exception ex) {
			context.getCounter("HBaseKVMapper", "PARSE_ERRORS").increment(1);
			return;
		}

//		hKey.set(fields[1].getBytes());

		// if (!fields[0].equals("")) {
//		 KeyValue(rowkey, column_family, column, value)
//		 kv = new KeyValue(hKey.get(), fields[0].getBytes(),
//		 fields[2].getBytes(), fields[3].getBytes());
		// context.write(hKey, kv);
		// }
		ImmutableBytesWritable ibKey = new ImmutableBytesWritable(
				Bytes.toBytes(fields[1]));
		
//		 KeyValue(rowkey, column_family, column, value)
		 KeyValue kv = new KeyValue(ibKey.get(), fields[0].getBytes(),
		 fields[2].getBytes(), fields[3].getBytes());

//		Put row = new Put(Bytes.toBytes(fields[1]));
//		row.add(Bytes.toBytes(fields[0]), Bytes.toBytes(fields[2]),
//				Bytes.toBytes(fields[3]));
		
		context.write(ibKey, kv);
		
		context.getCounter("HBaseKVMapper", "NUM_MSGS").increment(1);

	}
}
