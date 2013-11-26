package lupos.cloud.pig.operator;

import java.util.ArrayList;

import lupos.cloud.pig.BagInformation;
import lupos.cloud.storage.util.CloudManagement;

public class PigDistinctOperator implements IPigOperator {
	private ArrayList<BagInformation> intermediateJoins;

	public String buildQuery(ArrayList<BagInformation> intermediateBags, boolean debug, ArrayList<PigFilterOperator> filterOps) {
		StringBuilder result = new StringBuilder();
		this.intermediateJoins = intermediateBags;

		if (debug) {
			result.append("-- Distinct: \n");
		}

		BagInformation curJoin = intermediateJoins.get(0);
		BagInformation newJoin = new BagInformation("INTERMEDIATE_BAG_"
				+ BagInformation.idCounter);

		result.append(newJoin.getName() + " = DISTINCT " + curJoin.getName());
		
		if (CloudManagement.PARALLEL_REDUCE_OPERATIONS > 1) {
			result.append(" PARALLEL " + CloudManagement.PARALLEL_REDUCE_OPERATIONS);
		}
		
		result.append( ";\n");

		if (debug) {
			result.append("\n");
		}

		newJoin.setPatternId(BagInformation.idCounter);
		newJoin.setJoinElements(curJoin.getJoinElements());
		newJoin.addAppliedFilters(curJoin.getAppliedFilters());
		newJoin.addBitVectors(curJoin.getBitVectors());

		intermediateJoins.remove(curJoin);
		intermediateJoins.add(newJoin);
		BagInformation.idCounter++;
		return result.toString();
	}
}
