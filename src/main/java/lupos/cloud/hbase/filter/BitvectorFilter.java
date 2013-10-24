package lupos.cloud.hbase.filter;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.pig.data.TupleFactory;

import java.io.DataOutput;
import java.io.IOException;
import java.io.DataInput;
import java.util.ArrayList;
import java17Dependencies.BitSet;

import lupos.cloud.hbase.bulkLoad.HBaseKVMapper;
import lupos.cloud.testing.BitvectorManager;

import com.google.common.base.Preconditions;

// In einer späteren Version vll. direkt in  HBase filtern
@Deprecated
public class BitvectorFilter extends FilterBase {

	protected byte[] byteBitVector1 = null;
	protected byte[] byteBitVector2 = null;
	BitSet bitvector1 = null;
	BitSet bitvector2 = null;

	public BitvectorFilter() {
		super();
	}

	public BitvectorFilter(final byte[] bitvector1) {
		this.byteBitVector1 = bitvector1;
		this.bitvector1 = fromByteArray(bitvector1);
	}

	public BitvectorFilter(final byte[] bitvector1, final byte[] bitvector2) {
		this.byteBitVector1 = bitvector1;
		this.byteBitVector2 = bitvector1;
		this.bitvector1 = fromByteArray(bitvector1);
		this.bitvector2 = fromByteArray(bitvector2);
	}

	public static BitSet fromByteArray(byte[] bytes) {
		return BitSet.valueOf(bytes);
	}

	@Override
	public ReturnCode filterKeyValue(KeyValue kv) {
		String toSplit = Bytes.toString(kv.getKey());
		if (toSplit.contains(",")) {
			// 1
			String toAdd1 = toSplit.substring(0, toSplit.indexOf(","));
			if (bitvector1 != null
					&& !isElementPartOfBitvector(toAdd1, bitvector1)) {
				return ReturnCode.NEXT_COL;
			}
			// 2
			String toAdd2 = toSplit.substring(toSplit.indexOf(",") + 1,
					toSplit.length());

			if (bitvector2 != null
					&& !isElementPartOfBitvector(toAdd2, bitvector2)) {
				return ReturnCode.NEXT_COL;
			}
		} else {
			String toAdd = Bytes.toString(kv.getKey());
			if (bitvector1 != null
					&& !isElementPartOfBitvector(toAdd, bitvector1)) {
				return ReturnCode.NEXT_COL;
			}
		}
		return ReturnCode.INCLUDE;
	}

	private boolean isElementPartOfBitvector(String element, BitSet bitvector) {
		Integer position = BitvectorManager.hash(element.getBytes());
		if (bitvector.get(position)) {
			return true;
		} else {
			return false;
		}
	}

	// public ReturnCode filterColumn(byte[] buffer, int qualifierOffset,
	// int qualifierLength) {
	// return ReturnCode.SEEK_NEXT_USING_HINT;
	// }

	// @Deprecated
	// public ReturnCode filterColumn2(byte[] buffer, int qualifierOffset,
	// int qualifierLength) {
	// if (qualifierLength < byteBitVector.length) {
	// int cmp = Bytes.compareTo(buffer, qualifierOffset, qualifierLength,
	// this.byteBitVector, 0, qualifierLength);
	// if (cmp <= 0) {
	// return ReturnCode.SEEK_NEXT_USING_HINT;
	// } else {
	// return ReturnCode.NEXT_ROW;
	// }
	// } else {
	// int cmp = Bytes.compareTo(buffer, qualifierOffset,
	// this.byteBitVector.length, this.byteBitVector, 0,
	// this.byteBitVector.length);
	// if (cmp < 0) {
	// return ReturnCode.SEEK_NEXT_USING_HINT;
	// } else if (cmp > 0) {
	// return ReturnCode.NEXT_ROW;
	// } else {
	// return ReturnCode.INCLUDE;
	// }
	// }
	// }

	// public static Filter createFilterFromArguments(
	// ArrayList<byte[]> filterArguments) {
	// Preconditions.checkArgument(filterArguments.size() == 1,
	// "Expected 1 but got: %s", filterArguments.size());
	// byte[] bits = ParseFilter.removeQuotesFromByteArray(filterArguments
	// .get(0));
	// return new BitvectorFilter(bits);
	// }

	public void write(DataOutput out) throws IOException {
		Bytes.writeByteArray(out, this.byteBitVector1);
	}

	//
	public void readFields(DataInput in) throws IOException {
		this.byteBitVector1 = Bytes.readByteArray(in);
		this.byteBitVector2 = Bytes.readByteArray(in);
	}

	//
	// public KeyValue getNextKeyHint(KeyValue kv) {
	// return KeyValue.createFirstOnRow(kv.getBuffer(), kv.getRowOffset(),
	// kv.getRowLength(), kv.getBuffer(), kv.getFamilyOffset(),
	// kv.getFamilyLength(), byteBitVector, 0, byteBitVector.length);
	// }

	@Override
	public String toString() {
		return this.getClass().getSimpleName();
	}
}
