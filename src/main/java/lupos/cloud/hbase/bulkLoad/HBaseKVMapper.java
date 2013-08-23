package lupos.cloud.hbase.bulkLoad;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import au.com.bytecode.opencsv.CSVParser;

/**
 * Mapper Klasse zum Laden großer Datenmengen in HBase. Dabei wird eine
 * CSV-Datei auslgesen und jede Zeile anschließend in das passende
 * Tabellenformat gebracht.
 */
public class HBaseKVMapper extends
		Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {

	/** The csv parser. */
	CSVParser csvParser = new CSVParser('\t');

	/** The table name. */
	String tableName = "";

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.
	 * Mapper.Context)
	 */
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration c = context.getConfiguration();
		tableName = c.get("hbase.table.name");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
	 * org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] fields = null; // {"a", "b", "c", "d"};

		try {
			fields = csvParser.parseLine(value.toString());
		} catch (Exception ex) {
			context.getCounter("HBaseKVMapper", "PARSE_ERRORS").increment(1);
			return;
		}

		ImmutableBytesWritable ibKey = new ImmutableBytesWritable(
				Bytes.toBytes(fields[1]));

		KeyValue kv = new KeyValue(ibKey.get(), fields[0].getBytes(),
				fields[2].getBytes(), fields[3].getBytes());

		context.write(ibKey, kv);

		context.getCounter("HBaseKVMapper", "NUM_MSGS").increment(1);

	}
}
