package lupos.cloud.pig.operator;

import java.util.ArrayList;

import lupos.cloud.pig.BagInformation;
import lupos.datastructures.items.Variable;
import lupos.engine.operators.multiinput.join.Join;

/**
 * Join Operator
 */
public class PigJoinOperator implements IPigOperator {

	/** Zwischenergebnisse. */
	private BagInformation newBag;

	/** Menge der zu joinenden Bags. */
	private ArrayList<BagInformation> multiInputist;

	/** Luposdate Join. */
	private Join join;

	/**
	 * Instantiates a new pig join operator.
	 * 
	 * @param newBag
	 *            the new join
	 * @param multiInputist
	 *            the multi inputist
	 * @param join
	 *            the join
	 */
	public PigJoinOperator(BagInformation newBag,
			ArrayList<BagInformation> multiInputist, Join join) {
		this.newBag = newBag;
		this.multiInputist = multiInputist;
		this.join = join;
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
		if (debug) {
			result.append(" -- JOIN:\n");
		}
		result.append(newBag.getName() + " = JOIN ");
		for (int i = 0; i < multiInputist.size(); i++) {
			if (i > 0) {
				result.append(", ");
			}

			result.append(multiInputist.get(i).getName() + " BY ");

			ArrayList<Variable> joinList = new ArrayList<Variable>(
					join.getIntersectionVariables());

			if (joinList.size() == 0) {
				throw new RuntimeException(
						"Es sind keine Intersection Variablen für die Operation Join vorhanden -> Abbruch!");
			}

			if (joinList.size() == 1) {
				result.append("$"
						+ multiInputist.get(i).getBagElements()
								.indexOf("?" + joinList.get(0).getName()));
			} else {
				int j = 0;
				result.append("(");
				for (Variable var : joinList) {
					if (j > 0) {
						result.append(",");
					}
					result.append("$"
							+ multiInputist.get(j).getBagElements()
									.indexOf("?" + var.getName()));
					j++;
				}
				result.append(")");
			}
		}
		result.append(";\n\n");

		ArrayList<String> joinElements = new ArrayList<String>();
		for (Variable var : join.getIntersectionVariables()) {
			joinElements.add("?" + var.getName());
		}

		boolean firstBag = true;
		for (BagInformation bag : multiInputist) {
			if (firstBag) {
				newBag.setJoinElements(bag.getBagElements());
				firstBag = false;
			} else {
				for (String var : bag.getBagElements()) {
					newBag.addBagElements(var);
				}
			}
		}
		return result.toString();
	}
}
