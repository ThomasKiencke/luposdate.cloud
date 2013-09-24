package lupos.cloud.pig.udfs;

import java.io.IOException;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Map.Entry;

import lupos.cloud.pig.udfs.HBaseLoadUDF.ColumnInfo;
import lupos.cloud.testing.BitvectorManager;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.pig.EvalFunc;
import org.apache.pig.FilterFunc;
import org.apache.pig.OrderedLoadFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;


public class BoundFilterUDF extends FilterFunc {

	@Override
	public Boolean exec(Tuple input) throws ExecException {
		Object element = input.get(0);
//		System.out.println("bla " + element);
		if (element == null) {
			return false;
		} else {
			return true;
		}
	}

}
