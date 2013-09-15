package lupos.cloud.pig.udfs;

import java.io.IOException;
import java.util.BitSet;
import java.util.Map;
import java.util.Map.Entry;

import lupos.cloud.hbase.bulkLoad.HBaseKVMapper;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.pig.EvalFunc;
import org.apache.pig.FilterFunc;
import org.apache.pig.OrderedLoadFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class BloomfilterUDF extends FilterFunc {

	@Override
	public Boolean exec(Tuple input) throws IOException {
		DataByteArray dbArray = (DataByteArray) input.get(0);
		BitSet bitvector = fromByteArray(dbArray.get());
		String element = (String) input.get(1);
		int hash = element.hashCode();
		if (hash < 0) {
			hash = hash * (-1);
		}
		Integer position = hash % HBaseKVMapper.VECTORSIZE;
		if (bitvector.get(position)) {
			return true;
		} else {
			return false;
		}
	}
	
	  public static BitSet fromByteArray(byte[] bytes) {
	    BitSet bits = new BitSet();
	    for (int i = 0; i < bytes.length * 8; i++) {
	      if ((bytes[bytes.length - i / 8 - 1] & (1 << (i % 8))) > 0) {
	        bits.set(i);
	      }
	    }
	    return bits;
	  }

}
