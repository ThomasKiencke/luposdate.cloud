package lupos.cloud.pig.operator;

import java.util.ArrayList;

import lupos.cloud.pig.BagInformation;
import lupos.cloud.pig.SinglePigQuery;

public class PigLimitOperator implements IPigOperator {
	private ArrayList<BagInformation> intermediateJoins;
	private int limit = -1 ;
	
	public PigLimitOperator(int limit) {
		this.limit = limit;
	}
	public String buildQuery(ArrayList<BagInformation> intermediateBags, boolean debug, ArrayList<PigFilterOperator> filterOps) {
		StringBuilder result = new StringBuilder();
		this.intermediateJoins = intermediateBags;

		if (debug) {
			result.append("-- Limit: " + limit + " \n");
		}

		BagInformation curJoin = intermediateJoins.get(0);
		BagInformation newJoin = new BagInformation("INTERMEDIATE_BAG_"
				+ BagInformation.idCounter);

		result.append(newJoin.getName() + " = LIMIT " + curJoin.getName()
				+ " " + this.limit +  ";\n");

		if (debug) {
			result.append("\n");
		}

		newJoin.setPatternId(BagInformation.idCounter);
		newJoin.setJoinElements(curJoin.getJoinElements());
		newJoin.addAppliedFilters(curJoin.getAppliedFilters());
		newJoin.mergeOptionalVariables(curJoin);
		newJoin.addBitVectors(curJoin.getBitVectors());

		intermediateJoins.remove(curJoin);
		intermediateJoins.add(newJoin);
		BagInformation.idCounter++;
		return result.toString();
	}
}
