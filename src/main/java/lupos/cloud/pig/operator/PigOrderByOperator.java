package lupos.cloud.pig.operator;

import java.util.ArrayList;

import lupos.cloud.pig.BagInformation;
import lupos.cloud.pig.SinglePigQuery;
import lupos.cloud.storage.util.CloudManagement;
import lupos.datastructures.items.Variable;
import lupos.engine.operators.singleinput.sort.Sort;
import lupos.engine.operators.singleinput.sort.comparator.ComparatorBindings;
import lupos.sparql1_1.Node;

public class PigOrderByOperator implements IPigOperator {
	private ArrayList<BagInformation> intermediateJoins;
	private Sort orderByLuposOperation = null;

	public PigOrderByOperator(Sort sort) {
		this.orderByLuposOperation = sort;
	}

	public String buildQuery(ArrayList<BagInformation> intermediateBags, boolean debug, ArrayList<PigFilterOperator> filterOps) {
		StringBuilder result = new StringBuilder();
		this.intermediateJoins = intermediateBags;
		
		ArrayList<Variable> list = new ArrayList<Variable>(orderByLuposOperation.getSortCriterium());

		if (debug) {
			result.append("-- ORDER BY ?" + list.get(0).getName()  + "\n");
		}

		BagInformation curJoin = intermediateJoins.get(0);
		BagInformation newJoin = new BagInformation("INTERMEDIATE_BAG_"
				+ BagInformation.idCounter);

		// TODO: add ASC/DESC choice
		ComparatorBindings comparator = orderByLuposOperation.getComparator();
		
		
		result.append(newJoin.getName() + " = ORDER " + curJoin.getName() + " BY"
				+ " $" + curJoin.getItemPos("?" + list.get(0).getName()));
		
		if (CloudManagement.PARALLEL_REDUCE_OPERATIONS > 1) {
			result.append(" PARALLEL " + CloudManagement.PARALLEL_REDUCE_OPERATIONS);
		}
		
		result.append(";\n");

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
