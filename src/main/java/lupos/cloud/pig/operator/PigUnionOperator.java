package lupos.cloud.pig.operator;

import java.util.ArrayList;

import lupos.cloud.pig.BagInformation;
import lupos.cloud.pig.SinglePigQuery;

public class PigUnionOperator implements IPigOperator {
	private boolean debug;
	private BagInformation newJoin;
	private ArrayList<BagInformation> multiInputist;

	public PigUnionOperator(BagInformation newJoin,
			ArrayList<BagInformation> multiInputist) {
		this.newJoin = newJoin;
		this.multiInputist = multiInputist;
	}

	public String buildQuery(ArrayList<BagInformation> intermediateBags,
			boolean debug, ArrayList<PigFilterOperator> filterOps) {
		this.debug = debug;
		StringBuilder result = new StringBuilder();
		if (debug) {
			result.append("-- UNION:\n");
		}
		result.append(newJoin.getName() + " = UNION ");
		;
		for (int i = 0; i < multiInputist.size(); i++) {
			if (i == 0) {
				result.append(multiInputist.get(i).getName());
			} else {
				result.append(", " + multiInputist.get(i).getName());
			}
		}
		result.append(";\n\n");
		newJoin.setJoinElements(multiInputist.get(0).getJoinElements());

		for (BagInformation bag : multiInputist) {
			for (String elem : bag.getJoinElements()) {
				if (!multiInputist.get(0).getJoinElements().contains(elem)) {
					newJoin.addBagElements(elem);
					newJoin.addOptionalElements(elem);
				}
			}
		}

		return result.toString();
	}
}
