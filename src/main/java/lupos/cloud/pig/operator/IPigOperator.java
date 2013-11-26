package lupos.cloud.pig.operator;

import java.util.ArrayList;

import lupos.cloud.pig.BagInformation;
import lupos.cloud.pig.SinglePigQuery;

public interface IPigOperator {
	public String buildQuery(ArrayList<BagInformation> intermediateBags, boolean debug, ArrayList<PigFilterOperator> filterOps);
}
