package lupos.cloud.hbase.bulkLoad;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.bloom.HashFunction;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;
import org.python.antlr.PythonParser.return_stmt_return;

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
	
	public static final int VECTORSIZE = 10000000;

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

		if (fields.length < 4) {
			context.getCounter("HBaseKVMapper", "TRIPLE_ERROR").increment(1);
			return;
		}

		ImmutableBytesWritable ibKey = new ImmutableBytesWritable(
				Bytes.toBytes(fields[1]));

		// S P O Content
		KeyValue kv1 = new KeyValue(ibKey.get(), fields[0].getBytes(),
				fields[2].getBytes(), fields[3].getBytes());

		context.write(ibKey, kv1);

		String toSplit = fields[2];
		String elem1 = null;
		String elem2 = null;
		if (toSplit.contains(",")) {
			elem1 = toSplit.substring(0, toSplit.indexOf(","));
			elem2 = toSplit.substring(toSplit.indexOf(",") + 1,
					toSplit.length());
		} else {
			elem1 = toSplit.substring(0, toSplit.length());
		}
		// Bloomfilter
		if (!(elem1 == null)) {
			int hash = elem1.hashCode();
			if (hash < 0) {
				hash = hash * (-1);
			}
			Integer position = hash % VECTORSIZE;
			KeyValue kv2 = new KeyValue(ibKey.get(), "bloomfilter1".getBytes(),
					IntegerToByteArray(position), "".getBytes());
			context.write(ibKey, kv2);
		}
		
		if (!(elem2 == null)) {
			int hash = elem2.hashCode();
			if (hash < 0) {
				hash = hash * (-1);
			}
			Integer position = hash % VECTORSIZE;
			KeyValue kv2 = new KeyValue(ibKey.get(), "bloomfilter2".getBytes(),
					IntegerToByteArray(position), "".getBytes());
			context.write(ibKey, kv2);
		}

		
		context.getCounter("HBaseKVMapper", "TRIPLE_IMPORTED").increment(1);

	}
	
	public static byte[] IntegerToByteArray(Integer pos) {
		return ByteBuffer.allocate(4).putInt(pos).array();
	}
}
