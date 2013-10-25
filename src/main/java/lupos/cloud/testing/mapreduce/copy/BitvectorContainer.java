package lupos.cloud.testing.mapreduce.copy;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

import java17Dependencies.BitSet;

public class BitvectorContainer implements Writable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6403623676839769263L;

	BitSet bv1 = null;
	BitSet bv2 = null;

	public BitvectorContainer(BitSet bv1, BitSet bv2) {
		this.bv1 = bv1;
		this.bv2 = bv2;
	}

	public BitvectorContainer(String input) {
		// if (input.contains("x") {
		// bv1.toStr
		// } else
		// this.bv1 = bv1;
		// this.bv2 = bv2;
	}

	public BitSet getBv1() {
		return bv1;
	}

	public BitSet getBv2() {
		return bv2;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Bytes.writeByteArray(out, bv1.toByteArray());
		if (bv2 == null) {
			bv2 = new BitSet(1);
			Bytes.writeByteArray(out, bv2.toByteArray());
		}

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.bv1 = BitSet.valueOf(Bytes.readByteArray(in));
		this.bv2 = BitSet.valueOf(Bytes.readByteArray(in));
		if (bv2.length() == 0) {
			this.bv2 = null;
		}

	}

}
