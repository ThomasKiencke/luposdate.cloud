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
public class Old_ListToBagUDF extends EvalFunc<DataBag> implements OrderedLoadFunc {

	/** The Constant bagFactory. */
	private static final BagFactory bagFactory = BagFactory.getInstance();

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.pig.EvalFunc#exec(org.apache.pig.data.Tuple)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public DataBag exec(Tuple input) throws IOException {
		try {
			DataBag result = bagFactory.newDefaultBag();
			List<Tuple> list = (List<Tuple>) input.get(0);
			
			for (Tuple elem : list) {
				result.add(elem);
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
