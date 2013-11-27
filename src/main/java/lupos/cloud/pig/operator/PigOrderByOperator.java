package lupos.cloud.pig.operator;

import java.util.ArrayList;

import lupos.cloud.pig.BagInformation;
import lupos.cloud.storage.util.CloudManagement;
import lupos.datastructures.items.Variable;
import lupos.engine.operators.singleinput.sort.Sort;

/**
 * Order By Operator.
 */
public class PigOrderByOperator implements IPigOperator {

	/** Zwischenergebnisse. */
	private ArrayList<BagInformation> intermediateJoins;

	/** Luposdate Operator. */
	private Sort orderByLuposOperation = null;

	/**
	 * Instantiates a new pig order by operator.
	 * 
	 * @param sort
	 *            the sort
	 */
	public PigOrderByOperator(Sort sort) {
		this.orderByLuposOperation = sort;
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

		ArrayList<Variable> list = new ArrayList<Variable>(
				orderByLuposOperation.getSortCriterium());

		if (debug) {
			result.append("-- ORDER BY ?" + list.get(0).getName() + "\n");
		}

		BagInformation curBag = intermediateJoins.get(0);
		BagInformation newBag = new BagInformation("INTERMEDIATE_BAG_"
				+ BagInformation.idCounter);

		// TODO: add ASC/DESC choice
//		ComparatorBindings comparator = orderByLuposOperation.getComparator();

		result.append(newBag.getName() + " = ORDER " + curBag.getName()
				+ " BY" + " $"
				+ curBag.getItemPos("?" + list.get(0).getName()));

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
		newBag.mergeOptionalVariables(curBag);
		newBag.addBitVectors(curBag.getBitVectors());

		intermediateJoins.remove(curBag);
		intermediateJoins.add(newBag);
		BagInformation.idCounter++;
		return result.toString();
	}
}
