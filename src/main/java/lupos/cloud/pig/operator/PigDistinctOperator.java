package lupos.cloud.pig.operator;

import java.util.ArrayList;

import lupos.cloud.pig.BagInformation;
import lupos.cloud.storage.util.CloudManagement;

/**
 * Pig Distinct Operator.
 */
public class PigDistinctOperator implements IPigOperator {

	/** The intermediate joins. */
	private ArrayList<BagInformation> intermediateJoins;

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
			result.append("-- Distinct: \n");
		}

		BagInformation curBag = intermediateJoins.get(0);
		BagInformation newBag = new BagInformation("INTERMEDIATE_BAG_"
				+ BagInformation.idCounter);

		result.append(newBag.getName() + " = DISTINCT " + curBag.getName());

		if (CloudManagement.PARALLEL_REDUCE_OPERATIONS > 1) {
			result.append(" PARALLEL "
					+ CloudManagement.PARALLEL_REDUCE_OPERATIONS);
		}

		result.append(";\n");

		if (debug) {
			result.append("\n");
		}

		newBag.setPatternId(BagInformation.idCounter);
		newBag.setJoinElements(curBag.getBagElements());
		newBag.addAppliedFilters(curBag.getAppliedFilters());
		newBag.addBitVectors(curBag.getBitVectors());

		intermediateJoins.remove(curBag);
		intermediateJoins.add(newBag);
		BagInformation.idCounter++;
		return result.toString();
	}
}
