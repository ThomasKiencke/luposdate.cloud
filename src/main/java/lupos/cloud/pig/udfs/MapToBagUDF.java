package lupos.cloud.pig.udfs;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.pig.EvalFunc;
import org.apache.pig.OrderedLoadFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * UDF Funktion für Pig. In dieser Klasse wird eine eingehen Map in eine "Bag"
 * überführt um diese dann weiter zu verarbeiten.
 */
public class MapToBagUDF extends EvalFunc<DataBag> implements OrderedLoadFunc {

	/** The Constant bagFactory. */
	private static final BagFactory bagFactory = BagFactory.getInstance();

	/** The Constant tupleFactory. */
	private static final TupleFactory tupleFactory = TupleFactory.getInstance();

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.pig.EvalFunc#exec(org.apache.pig.data.Tuple)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public DataBag exec(Tuple input) throws IOException {
		try {
			int mapSize = input.size();

			DataBag result = bagFactory.newDefaultBag();
			Tuple tuple = tupleFactory.newTuple(mapSize);

			if (mapSize == 1) {
				List<String> map1 = (List<String>) input.get(0);
				for (String entry : map1) {
					tuple.set(0, entry);
					result.add(tuple);
				}
			} else if (mapSize == 2) {
				List<String> map1 = (List<String>) input.get(0);
				List<String> map2 = (List<String>) input.get(1);
				if (map1.size() != map2.size()) {
					throw new RuntimeException(
							"MapToBag error: size not equals");
				}
				int i = 0;
				for (String entry : map1) {
					tuple.set(0, entry);
					tuple.set(1, map2.get(i));
					i++;
					result.add(tuple);
				}
			} else if (mapSize == 3) {
				List<String> map1 = (List<String>) input.get(0);
				List<String> map2 = (List<String>) input.get(1);
				List<String> map3 = (List<String>) input.get(2);
				if (map1.size() != map2.size()) {
					throw new RuntimeException(
							"MapToBag error: size not equals");
				}
				int i = 0;
				for (String entry : map1) {
					tuple.set(0, entry);
					tuple.set(1, map2.get(i));
					tuple.set(2, map3.get(i));
					i++;
					result.add(tuple);
				}
			}
			return result;

		} catch (Exception e) {
			throw new RuntimeException("MapToBag error", e);
		}
	}

	@Override
	public WritableComparable<?> getSplitComparable(InputSplit split)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}
}
