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
public class MapToBagUDF extends EvalFunc<DataBag> implements OrderedLoadFunc {

	/** The Constant bagFactory. */
	private static final BagFactory bagFactory = BagFactory.getInstance();
	private static final TupleFactory tupleFactory = TupleFactory.getInstance();

	private BitSet bitvector1 = null;
	private BitSet bitvector2 = null;

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.pig.EvalFunc#exec(org.apache.pig.data.Tuple)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public DataBag exec(Tuple input) throws IOException {
		DataBag result = bagFactory.newDefaultBag();
		try {
			Map<String, DataByteArray> cfMap = (HashMap<String, DataByteArray>) input
					.get(0);

			if (input.size() == 2) {
				Object b1 = input.get(1);
				if (b1 != null) {
					bitvector1 = (BitSet) b1;
				}
			}

			if (input.size() == 3) {
				Object b1 = input.get(1);
				if (b1 != null) {
					bitvector1 = (BitSet) b1;
				}
				Object b2 = input.get(2);
				if (b2 != null) {
					bitvector2 = (BitSet) b2;
				}
			}

			if (cfMap != null) {
				for (String quantifier : cfMap.keySet()) {
					String[] columnname = quantifier.split(",");

					if (columnname.length > 1) {
						if (bitvector1 != null
								&& !isElementPartOfBitvector(
										columnname[0].getBytes(), bitvector1)) {
							continue;
						}

						// 2
						if (bitvector2 != null
								&& !isElementPartOfBitvector(
										columnname[1].getBytes(), bitvector2)) {
							continue;
						}

					} else {
						if (bitvector1 != null
								&& !isElementPartOfBitvector(
										columnname[0].getBytes(), bitvector1)) {
							continue;
						}
					}

					if (columnname.length > 1) {
						Tuple toAdd = tupleFactory.newTuple(2);
						toAdd.set(0, columnname[0]);
						toAdd.set(1, columnname[1]);
						result.add(toAdd);
					} else {
						Tuple toAdd = tupleFactory.newTuple(1);
						toAdd.set(0, columnname[0]);
						result.add(toAdd);
					}
				}
			}

		} catch (Exception e) {
			throw new RuntimeException("MapToBag error", e);
		}
		return result;
	}

	@Override
	public WritableComparable<?> getSplitComparable(InputSplit split)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	private boolean isElementPartOfBitvector(byte[] element, BitSet bitvector) {
		Integer position = BitvectorManager.hash(element);
		if (bitvector.get(position)) {
			return true;
		} else {
			return false;
		}
	}
}
