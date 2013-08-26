package lupos.cloud.pig.operator;

import java.util.ArrayList;

import lupos.cloud.pig.JoinInformation;
import lupos.cloud.pig.PigQuery;

public class PigLimitOperator implements IPigOperator {
	private ArrayList<JoinInformation> intermediateJoins;
	private int limit = -1 ;
	
	public PigLimitOperator(int limit) {
		this.limit = limit;
	}
	public String buildQuery(PigQuery pigQuery) {
		StringBuilder result = new StringBuilder();
		this.intermediateJoins = pigQuery.getIntermediateJoins();

		if (pigQuery.isDebug()) {
			result.append("-- Limit: \n");
		}

		JoinInformation curJoin = intermediateJoins.get(0);
		JoinInformation newJoin = new JoinInformation("INTERMEDIATE_BAG_"
				+ JoinInformation.idCounter);

		result.append(newJoin.getName() + " = LIMIT " + curJoin.getName()
				+ " " + this.limit +  ";\n");

		if (pigQuery.isDebug()) {
			result.append("\n");
		}

		newJoin.setPatternId(JoinInformation.idCounter);
		newJoin.setJoinElements(curJoin.getJoinElements());
		newJoin.addAppliedFilters(curJoin.getAppliedFilters());

		intermediateJoins.remove(curJoin);
		intermediateJoins.add(newJoin);
		JoinInformation.idCounter++;
		return result.toString();
	}
}
