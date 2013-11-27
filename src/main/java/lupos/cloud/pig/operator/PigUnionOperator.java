package lupos.cloud.pig.operator;

import java.util.ArrayList;

import lupos.cloud.pig.BagInformation;

/**
 * Union Operator.
 */
public class PigUnionOperator implements IPigOperator {
	
	/** Zwischenergebnisse. */
	private BagInformation newBag;
	
	/** The multi inputist. */
	private ArrayList<BagInformation> multiInputist;

	/**
	 * Instantiates a new pig union operator.
	 *
	 * @param newJoin the new join
	 * @param multiInputist the multi inputist
	 */
	public PigUnionOperator(BagInformation newJoin,
			ArrayList<BagInformation> multiInputist) {
		this.newBag = newJoin;
		this.multiInputist = multiInputist;
	}

	/* (non-Javadoc)
	 * @see lupos.cloud.pig.operator.IPigOperator#buildQuery(java.util.ArrayList, boolean, java.util.ArrayList)
	 */
	public String buildQuery(ArrayList<BagInformation> intermediateBags,
			boolean debug, ArrayList<PigFilterOperator> filterOps) {
		StringBuilder result = new StringBuilder();
		if (debug) {
			result.append("-- UNION:\n");
		}
		result.append(newBag.getName() + " = UNION ");
		;
		for (int i = 0; i < multiInputist.size(); i++) {
			if (i == 0) {
				result.append(multiInputist.get(i).getName());
			} else {
				result.append(", " + multiInputist.get(i).getName());
			}
		}
		result.append(";\n\n");
		newBag.setJoinElements(multiInputist.get(0).getBagElements());

		for (BagInformation bag : multiInputist) {
			for (String elem : bag.getBagElements()) {
				if (!multiInputist.get(0).getBagElements().contains(elem)) {
					newBag.addBagElements(elem);
					newBag.addOptionalElements(elem);
				}
			}
		}

		return result.toString();
	}
}
