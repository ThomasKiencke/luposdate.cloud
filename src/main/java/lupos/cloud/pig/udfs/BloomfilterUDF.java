package lupos.cloud.pig.udfs;

import java.io.IOException;
import java.util.BitSet;

import lupos.cloud.hbase.bulkLoad.HBaseKVMapper;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

public class BloomfilterUDF extends FilterFunc {
	public BitSet bitvector = null;

	@Override
	public Boolean exec(Tuple input) throws IOException {
		if (bitvector == null) {
			DataByteArray dbArray = (DataByteArray) input.get(0);
			bitvector = fromByteArray(dbArray.get());
		}
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
