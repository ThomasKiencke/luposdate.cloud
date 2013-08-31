package lupos.cloud.pig.operator;

import java.util.ArrayList;

import lupos.cloud.pig.JoinInformation;
import lupos.cloud.pig.SinglePigQuery;

public interface IPigOperator {
	public String buildQuery(ArrayList<JoinInformation> intermediateBags, boolean debug, ArrayList<PigFilterOperator> filterOps);
}
