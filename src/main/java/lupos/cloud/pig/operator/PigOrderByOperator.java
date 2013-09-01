package lupos.cloud.pig.operator;

import java.util.ArrayList;

import lupos.cloud.pig.JoinInformation;
import lupos.cloud.pig.SinglePigQuery;
import lupos.datastructures.items.Variable;
import lupos.engine.operators.singleinput.sort.Sort;
import lupos.engine.operators.singleinput.sort.comparator.ComparatorBindings;
import lupos.sparql1_1.Node;

public class PigOrderByOperator implements IPigOperator {
	private ArrayList<JoinInformation> intermediateJoins;
	private Sort orderByLuposOperation = null;
	
	public PigOrderByOperator(Sort sort) {
		this.orderByLuposOperation = sort;
	}
	public String buildQuery(ArrayList<JoinInformation> intermediateBags, boolean debug, ArrayList<PigFilterOperator> filterOps) {
		StringBuilder result = new StringBuilder();
		this.intermediateJoins = intermediateBags;
		
		ArrayList<Variable> list = new ArrayList<Variable>(orderByLuposOperation.getSortCriterium());

		if (debug) {
			result.append("-- ORDER BY ?" + list.get(0).getName()  + "\n");
		}

		JoinInformation curJoin = intermediateJoins.get(0);
		JoinInformation newJoin = new JoinInformation("INTERMEDIATE_BAG_"
				+ JoinInformation.idCounter);

		// TODO: add ASC/DESC choice
		ComparatorBindings comparator = orderByLuposOperation.getComparator();
		
		
		result.append(newJoin.getName() + " = ORDER " + curJoin.getName() + " BY"
				+ " $" + curJoin.getItemPos("?" + list.get(0).getName()) + ";\n");

		if (debug) {
			result.append("\n");
		}

		newJoin.setPatternId(JoinInformation.idCounter);
		newJoin.setJoinElements(curJoin.getJoinElements());
		newJoin.addAppliedFilters(curJoin.getAppliedFilters());
		newJoin.mergeOptionalVariables(curJoin);

		intermediateJoins.remove(curJoin);
		intermediateJoins.add(newJoin);
		JoinInformation.idCounter++;
		return result.toString();
	}
}
