package lupos.cloud.pig.udfs;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;
import java.util.Map;
import java.util.Map.Entry;

import lupos.cloud.hbase.bulkLoad.HBaseKVMapper;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.pig.EvalFunc;
import org.apache.pig.OrderedLoadFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * UDF Funktion für Pig. In dieser Klasse wird eine eingehen Map in eine "Bag"
 * überführt um diese dann weiter zu verarbeiten.
 */
public class BloomfilterMapToByteArrayUDF extends EvalFunc<DataByteArray> {

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.pig.EvalFunc#exec(org.apache.pig.data.Tuple)
	 */
	@Override
	public DataByteArray exec(Tuple input) throws IOException {
			@SuppressWarnings("unchecked")
			Map<String, Object> map = (Map<String, Object>) input.get(0);
			BitSet bitvector = new BitSet(HBaseKVMapper.VECTORSIZE);;
			if (map != null) {
				for (Entry<String, Object> entry : map.entrySet()) {
					Integer pos = Integer.parseInt(entry.getKey());
					bitvector.set(pos);
				}
			}

			return new DataByteArray(toByteArray(bitvector));
	}
	
	public static byte[] toByteArray(BitSet bits) {
	    byte[] bytes = new byte[bits.length()/8+1];
	    for (int i=0; i<bits.length(); i++) {
	        if (bits.get(i)) {
	            bytes[bytes.length-i/8-1] |= 1<<(i%8);
	        }
	    }
	    return bytes;
	}
}
