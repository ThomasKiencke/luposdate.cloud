package lupos.cloud.pig.udfs;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * UDF Funktion für Pig. In dieser Klasse wird eine eingehen Map in eine "Bag"
 * überführt um diese dann weiter zu verarbeiten.
 */
public class MapToBag extends EvalFunc<DataBag> {

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
	public DataBag exec(Tuple input) throws IOException {
		try {
			@SuppressWarnings("unchecked")
			Map<String, Object> map = (Map<String, Object>) input.get(0);
			DataBag result = null;
			Tuple tuple = null;
			if (map != null) {
				result = bagFactory.newDefaultBag();
				for (Entry<String, Object> entry : map.entrySet()) {
					String toSplit = entry.getKey().toString();
					if (toSplit.contains(",")) {
						tuple = tupleFactory.newTuple(2);
						tuple.set(0, toSplit.substring(0, toSplit.indexOf(",")));
						tuple.set(1, toSplit.substring(
								toSplit.indexOf(",") + 1, toSplit.length()));
					} else {
						tuple = tupleFactory.newTuple(1);
						tuple.set(0, toSplit.substring(0, toSplit.length()));
					}

					result.add(tuple);
				}
			}
			return result;

		} catch (Exception e) {
			throw new RuntimeException("MapToBag error", e);
		}
	}
}
