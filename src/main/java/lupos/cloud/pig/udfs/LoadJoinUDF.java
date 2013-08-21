package lupos.cloud.pig.udfs;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

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
		
		return result;

	}
}
