package lupos.cloud.pig.udfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.hbase.HBaseStorage;
import org.apache.pig.backend.hadoop.hbase.HBaseStorage.ColumnInfo;
import org.apache.pig.data.BinSedesTuple;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class PigLoadUDF extends HBaseStorage {
	private RecordReader reader;

	@Override
	public void prepareToRead(RecordReader reader, PigSplit split) {
		super.prepareToRead(reader, split);
		this.reader = reader;
	}

	public PigLoadUDF(String columnList, String optString)
			throws ParseException, IOException {
		super(columnList, optString);
	}

	public PigLoadUDF(String columnList) throws ParseException, IOException {
		this(columnList, "");
	}

	 @Override
	public Tuple getNext() throws IOException {
		try {
			if (reader.nextKeyValue()) {
				ImmutableBytesWritable rowKey = (ImmutableBytesWritable) reader
						.getCurrentKey();
				Result result = (Result) reader.getCurrentValue();

				int tupleSize = 2;

				// use a map of families -> qualifiers with the most recent
				// version of the cell. Fetching multiple vesions could be a
				// useful feature.
				NavigableMap<byte[], NavigableMap<byte[], byte[]>> resultsMap = result
						.getNoVersionMap();

				if (true) {
					tupleSize++;
				}
				Tuple tuple = TupleFactory.getInstance().newTuple(tupleSize);

				int startIndex = 0;
				if (true) {
					tuple.set(0, new DataByteArray(rowKey.get()));
					startIndex++;
				}
				for (int i = 0; i < 2; ++i) {
					int currentIndex = startIndex + i;

					if (true) {
						// It's a column family so we need to iterate and set
						// all
						// values found

						NavigableMap<byte[], byte[]> cfResults = resultsMap
								.get("VALUE".getBytes());
						Map<String, DataByteArray> cfMap = new HashMap<String, DataByteArray>();

						if (cfResults != null) {
							for (byte[] quantifier : cfResults.keySet()) {
								// We need to check against the prefix filter to
								// see if this value should be included. We
								// can't
								// just rely on the server-side filter, since a
								// user could specify multiple CF filters for
								// the
								// same CF.
								if (true) {
									byte[] cell = cfResults.get(quantifier);
									DataByteArray value = cell == null ? null
											: new DataByteArray(cell);
									cfMap.put(Bytes.toString(quantifier), value);
								}
							}
						}
						tuple.set(currentIndex, cfMap);
					} else {
						//
					}
				}

				return tuple;
			}
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
		return null;
	}
}
