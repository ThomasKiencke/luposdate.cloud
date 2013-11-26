package lupos.cloud.hbase.filter;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.DataOutput;
import java.io.IOException;
import java.io.DataInput;

import java17Dependencies.BitSet;
import lupos.cloud.bloomfilter.BitvectorManager;

// In einer späteren Version vll. direkt in  HBase filtern
/**
 * Mit dieser Klasse werden die Tripel direkt in HBase anhand des Bloomfilters
 * gefiltert
 * 
 * Anmerkung: Noch nicht vollständig.
 */
@Deprecated
public class BitvectorFilter extends FilterBase {

	/** The byte bit vector1. */
	protected byte[] byteBitVector1 = null;

	/** The byte bit vector2. */
	protected byte[] byteBitVector2 = null;

	/** The bitvector1. */
	BitSet bitvector1 = null;

	/** The bitvector2. */
	BitSet bitvector2 = null;

	/**
	 * Instantiates a new bitvector filter.
	 */
	public BitvectorFilter() {
		super();
	}

	/**
	 * Instantiates a new bitvector filter.
	 * 
	 * @param bitvector1
	 *            the bitvector1
	 */
	public BitvectorFilter(final byte[] bitvector1) {
		this.byteBitVector1 = bitvector1;
		this.bitvector1 = fromByteArray(bitvector1);
	}

	/**
	 * Instantiates a new bitvector filter.
	 * 
	 * @param bitvector1
	 *            the bitvector1
	 * @param bitvector2
	 *            the bitvector2
	 */
	public BitvectorFilter(final byte[] bitvector1, final byte[] bitvector2) {
		this.byteBitVector1 = bitvector1;
		this.byteBitVector2 = bitvector1;
		this.bitvector1 = fromByteArray(bitvector1);
		this.bitvector2 = fromByteArray(bitvector2);
	}

	/**
	 * From byte array.
	 * 
	 * @param bytes
	 *            the bytes
	 * @return the bit set
	 */
	public static BitSet fromByteArray(byte[] bytes) {
		return BitSet.valueOf(bytes);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.hadoop.hbase.filter.FilterBase#filterKeyValue(org.apache.hadoop
	 * .hbase.KeyValue)
	 */
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

	/**
	 * Checks if is element part of bitvector.
	 * 
	 * @param element
	 *            the element
	 * @param bitvector
	 *            the bitvector
	 * @return true, if is element part of bitvector
	 */
	private boolean isElementPartOfBitvector(String element, BitSet bitvector) {
		Integer position = BitvectorManager.hash(element.getBytes());
		if (bitvector.get(position)) {
			return true;
		} else {
			return false;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	public void write(DataOutput out) throws IOException {
		Bytes.writeByteArray(out, this.byteBitVector1);
	}

	//
	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	public void readFields(DataInput in) throws IOException {
		this.byteBitVector1 = Bytes.readByteArray(in);
		this.byteBitVector2 = Bytes.readByteArray(in);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.hbase.filter.FilterBase#toString()
	 */
	@Override
	public String toString() {
		return this.getClass().getSimpleName();
	}
}
