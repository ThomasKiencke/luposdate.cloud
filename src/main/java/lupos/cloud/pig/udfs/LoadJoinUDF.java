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
				.toString(), input.get(2).toString(), input.get(3).toString(),
				(input.size() == 6) ? input.get(4).toString()
						+ input.get(5).toString() : input.get(4).toString());
		if (curTuple != null) {
			result.add(curTuple);
		}
		return result;

	}

	public Tuple getTuple(String printRowKey, String cf, String tablename,
			String rowKey, String sideWayInformation) {
		try {
			// ImmutableBytesWritable rowKey = (ImmutableBytesWritable)
			// reader
			// .getCurrentKey();
			// Result result = (Result) reader.getCurrentValue();

			Result result = (Result) HBaseConnection.getRowWithColumn(
					tablename, rowKey, cf, sideWayInformation);
			if (result.isEmpty()) {
				return null;
			}

			int tupleSize = -1;
			// int tupleSize = columnInfo_.size();
			if (printRowKey.equals("true")) {
				tupleSize = 2;
			} else {
				tupleSize = 1;
			}

			// use a map of families -> qualifiers with the most recent
			// version of the cell. Fetching multiple vesions could be a
			// useful feature.
			NavigableMap<byte[], NavigableMap<byte[], byte[]>> resultsMap = result
					.getNoVersionMap();

			if (true) {
				tupleSize++;
			}
			Tuple tuple = TupleFactory.getInstance().newTuple(tupleSize);

			NavigableMap<byte[], byte[]> cfResults = resultsMap.get(cf
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
			if (printRowKey.equals("true")) {
				tuple.set(0, rowKey);
				tuple.set(1, cfMap);
			} else {
				tuple.set(0, cfMap);
			}

			return tuple;
			// }
		} catch (ExecException e) {
			e.printStackTrace();
		}
		return null;
	}
}
