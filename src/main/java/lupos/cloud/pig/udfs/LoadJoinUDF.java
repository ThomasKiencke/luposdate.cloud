package lupos.cloud.pig.udfs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Map.Entry;

import lupos.cloud.hbase.HBaseConnection;
import lupos.cloud.pig.udfs.PigLoadInformationPassingUDF.ColumnInfo;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class LoadJoinUDF extends EvalFunc<DataBag> {

	private static final BagFactory bagFactory = BagFactory.getInstance();
	private static final TupleFactory tupleFactory = TupleFactory.getInstance();

	@Override
	public DataBag exec(Tuple input) throws IOException {
		DataBag result = null;
		// System.out.println("a: " + input.get(0) + "b: " + input.get(1));
		result = bagFactory.newDefaultBag();
		Tuple curTuple = getTuple(input.get(0).toString(), input.get(1)
				.toString());
		result.add(curTuple);
		return result;

	}

	public Tuple getTuple(String tablename, String rowKey) {
		try {
			// ImmutableBytesWritable rowKey = (ImmutableBytesWritable)
			// reader
			// .getCurrentKey();
			// Result result = (Result) reader.getCurrentValue();

			Result result = (Result) HBaseConnection.getRow(tablename, rowKey);

			// int tupleSize = columnInfo_.size();
			int tupleSize = 2;

			// use a map of families -> qualifiers with the most recent
			// version of the cell. Fetching multiple vesions could be a
			// useful feature.
			NavigableMap<byte[], NavigableMap<byte[], byte[]>> resultsMap = result
					.getNoVersionMap();

			if (true) {
				tupleSize++;
			}
			Tuple tuple = TupleFactory.getInstance().newTuple(1);

//			int startIndex = 0;
			// if (true) {
			// tuple.set(0, new DataByteArray(rowKey.getBytes()));
			// startIndex++;
			// }
			// for (int i = 0; i < 2; ++i) {
//			int currentIndex = startIndex + i;

			// It's a column family so we need to iterate and set
			// all
			// values found
			NavigableMap<byte[], byte[]> cfResults = resultsMap.get("VALUE"
					.getBytes());
			Map<String, DataByteArray> cfMap = new HashMap<String, DataByteArray>();

			if (cfResults != null) {
				for (byte[] quantifier : cfResults.keySet()) {
					byte[] cell = cfResults.get(quantifier);
					DataByteArray value = cell == null ? null
							: new DataByteArray(cell);
					cfMap.put(Bytes.toString(quantifier), value);

				}
			}
			tuple.set(0, cfMap);

			return tuple;
			// }
		} catch (ExecException e) {
			e.printStackTrace();
		}
		return null;
	}
}
