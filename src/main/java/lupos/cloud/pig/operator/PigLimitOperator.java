package lupos.cloud.pig.operator;

import java.util.ArrayList;

import lupos.cloud.pig.JoinInformation;
import lupos.cloud.pig.SinglePigQuery;

public class PigLimitOperator implements IPigOperator {
	private ArrayList<JoinInformation> intermediateJoins;
	private int limit = -1 ;
	
	public PigLimitOperator(int limit) {
		this.limit = limit;
	}
	public String buildQuery(ArrayList<JoinInformation> intermediateBags, boolean debug, ArrayList<PigFilterOperator> filterOps) {
		StringBuilder result = new StringBuilder();
		this.intermediateJoins = intermediateBags;

		if (debug) {
			result.append("-- Limit: " + limit + " \n");
		}

		JoinInformation curJoin = intermediateJoins.get(0);
		JoinInformation newJoin = new JoinInformation("INTERMEDIATE_BAG_"
				+ JoinInformation.idCounter);

		result.append(newJoin.getName() + " = LIMIT " + curJoin.getName()
				+ " " + this.limit +  ";\n");

		if (debug) {
			result.append("\n");
		}

		newJoin.setPatternId(JoinInformation.idCounter);
		newJoin.setJoinElements(curJoin.getJoinElements());
		newJoin.addAppliedFilters(curJoin.getAppliedFilters());
		newJoin.mergeOptionalVariables(curJoin);
		newJoin.addBitVectors(curJoin.getBitVectors());

		intermediateJoins.remove(curJoin);
		intermediateJoins.add(newJoin);
		JoinInformation.idCounter++;
		return result.toString();
	}
}
