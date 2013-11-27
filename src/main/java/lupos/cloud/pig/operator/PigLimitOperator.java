package lupos.cloud.pig.operator;

import java.util.ArrayList;

import lupos.cloud.pig.BagInformation;

/**
 * Limit Operator.
 */
public class PigLimitOperator implements IPigOperator {

	/** Zwischenergebnisse. */
	private ArrayList<BagInformation> intermediateJoins;

	/** Limit. */
	private int limit = -1;

	/**
	 * Instantiates a new pig limit operator.
	 * 
	 * @param limit
	 *            the limit
	 */
	public PigLimitOperator(int limit) {
		this.limit = limit;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * lupos.cloud.pig.operator.IPigOperator#buildQuery(java.util.ArrayList,
	 * boolean, java.util.ArrayList)
	 */
	public String buildQuery(ArrayList<BagInformation> intermediateBags,
			boolean debug, ArrayList<PigFilterOperator> filterOps) {
		StringBuilder result = new StringBuilder();
		this.intermediateJoins = intermediateBags;

		if (debug) {
			result.append("-- Limit: " + limit + " \n");
		}

		BagInformation curBag = intermediateJoins.get(0);
		BagInformation newBag = new BagInformation("INTERMEDIATE_BAG_"
				+ BagInformation.idCounter);

		result.append(newBag.getName() + " = LIMIT " + curBag.getName() + " "
				+ this.limit + ";\n");

		if (debug) {
			result.append("\n");
		}

		newBag.setPatternId(BagInformation.idCounter);
		newBag.setJoinElements(curBag.getBagElements());
		newBag.addAppliedFilters(curBag.getAppliedFilters());
		newBag.mergeOptionalVariables(curBag);
		newBag.addBitVectors(curBag.getBitVectors());

		intermediateJoins.remove(curBag);
		intermediateJoins.add(newBag);
		BagInformation.idCounter++;
		return result.toString();
	}
}
