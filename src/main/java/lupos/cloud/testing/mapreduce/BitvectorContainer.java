package lupos.cloud.testing.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
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
//	HashSet<Integer> indexListBV1 = new HashSet<Integer>();
//	HashSet<Integer> indexListBV2 = new HashSet<Integer>();
	Result result = null;
	
	public BitvectorContainer(BitSet bv1, BitSet bv2) {
		this.bv1 = bv1;
		this.bv2 = bv2;
	}
	
	public BitvectorContainer() {
		
	}
	
	public void setResult(Result result) {
		this.result = result;
	}
	
	public Result getResult() {
		return result;
	}
	
//	public void addIndexBV1(Integer toAdd) {
//		this.indexListBV1.add(toAdd);
//	}
//	
//	public void addIndexBV2(Integer toAdd) {
//		this.indexListBV2.add(toAdd);
//	}

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
		Bytes.writeByteArray(out, result.getBytes().copyBytes());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.bv1 = BitSet.valueOf(Bytes.readByteArray(in));
		this.bv2 = BitSet.valueOf(Bytes.readByteArray(in));
		if (bv2.length() == 0) {
			this.bv2 = null;
		}
		result = new Result(new ImmutableBytesWritable(Bytes.readByteArray(in)));

	}

}
