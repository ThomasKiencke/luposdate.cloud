package lupos.cloud.pig;

import java.util.ArrayList;

import lupos.cloud.pig.operator.IPigOperator;
import lupos.cloud.pig.operator.PigJoinOperator;

public class PigQuery {

	ArrayList<SinglePigQuery> singleQueries = new ArrayList<SinglePigQuery>();
	ArrayList<JoinInformation> intermediateBags = new ArrayList<JoinInformation>();
	StringBuilder pigLatin = new StringBuilder();
	public static boolean debug = true;

	// public void applyJoins() {
	// // TODO Auto-generated method stub
	//
	// }

	public void finishQuery() {
		StringBuilder modifiedPigQuery = new StringBuilder();
		modifiedPigQuery.append(this.pigLatin.toString().replace(
				this.getFinalAlias(), "X"));
		this.pigLatin = modifiedPigQuery;
	}

	public String getPigLatin() {
		return pigLatin.toString();
	}

	public ArrayList<String> getVariableList() {
		return singleQueries.get(0).getVariableList();
	}

	public void addAndPrceedSinglePigQuery(SinglePigQuery singlePigQuery) {
		singlePigQuery.finishQuery();
		this.singleQueries.add(singlePigQuery);
		intermediateBags.add(singlePigQuery.getIntermediateJoins().get(0));
		pigLatin.append(singlePigQuery.getPigLatin());
	}

	public void buildAndAppendQuery(IPigOperator operator) {
		this.pigLatin
				.append(operator.buildQuery(intermediateBags, debug, null));
	}

	public void removeIntermediateBags(JoinInformation toRemove) {
		this.intermediateBags.remove(toRemove);
	}

	public void addIntermediateBags(JoinInformation newJoin) {
		this.intermediateBags.add(newJoin);
	}

	public JoinInformation getLastAddedBag() {
		JoinInformation result = null;
		result = intermediateBags.get(intermediateBags.size() -1 );
		return result;
	}
	
	public String getFinalAlias() {
		return intermediateBags.get(0).getName();
	}
}
