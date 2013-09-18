package lupos.cloud.pig.udfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;

import lupos.cloud.hbase.bulkLoad.HBaseKVMapper;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

/**
 * UDF Funktion für Pig. In dieser Klasse wird eine eingehen Map in eine "Bag"
 * überführt um diese dann weiter zu verarbeiten.
 */
public class MergeBloomfilterUDF extends EvalFunc<DataByteArray> {

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.pig.EvalFunc#exec(org.apache.pig.data.Tuple)
	 */
	@Override
	public DataByteArray exec(Tuple input) throws IOException {
		ArrayList<BitSet> bitSetList = new ArrayList<BitSet>();

		for (int i = 0; i < input.size(); i++) {
			bitSetList.add(fromByteArray(((DataByteArray) input.get(i)).get()));
		}

		BitSet bitvector = new BitSet(HBaseKVMapper.VECTORSIZE);
		for (int i = 0; i < bitvector.size(); i++) {
			boolean setBit = true;
			for (BitSet bits : bitSetList) {
				if (!bits.get(i)) {
					setBit = false;
				}
			}
			if (setBit) {
				bitvector.set(i);
			}
		}
		return new DataByteArray(toByteArray(bitvector));
	}

	public static byte[] toByteArray(BitSet bits) {
		byte[] bytes = new byte[bits.length() / 8 + 1];
		for (int i = 0; i < bits.length(); i++) {
			if (bits.get(i)) {
				bytes[bytes.length - i / 8 - 1] |= 1 << (i % 8);
			}
		}
		return bytes;
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
