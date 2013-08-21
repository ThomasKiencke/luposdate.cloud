package lupos.cloud.pig.udfs;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.cli.ParseException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class LoadJoinUDF extends EvalFunc<DataBag> {

	private static final BagFactory bagFactory = BagFactory.getInstance();
	private static final TupleFactory tupleFactory = TupleFactory.getInstance();

	@Override
	public DataBag exec(Tuple input) throws IOException {
		DataBag result = null;
		try {
			result = bagFactory.newDefaultBag();
			PigLoadUDF loader = new PigLoadUDF("VALUE", "-loadKey true", input
					.get(0).toString());
			loader.getInputFormat();
			Tuple curTuple = loader.getNext();
			
			while (curTuple != null) {
				result.add(curTuple);
				curTuple = loader.getNext();
			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;

	}
}
