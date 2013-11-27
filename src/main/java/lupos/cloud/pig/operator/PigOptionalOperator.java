package lupos.cloud.pig.operator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

import lupos.cloud.pig.BagInformation;
import lupos.datastructures.items.Variable;
import lupos.engine.operators.multiinput.optional.Optional;

/**
 * Optional Operator.
 */
public class PigOptionalOperator implements IPigOperator {

	/** Zwischenergebnisse. */
	private BagInformation newBag;

	/** Eingangsmengen. */
	private ArrayList<BagInformation> multiInputist;

	/**
	 * Instantiates a new pig optional operator.
	 * 
	 * @param newBag
	 *            the new bag
	 * @param multiInputist
	 *            the multi inputist
	 * @param optional
	 *            the optional
	 */
	public PigOptionalOperator(BagInformation newBag,
			ArrayList<BagInformation> multiInputist, Optional optional) {
		this.newBag = newBag;
		this.multiInputist = multiInputist;
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

		ArrayList<String> intersectionVariables = this
				.getIntersectionVariables(
						multiInputist.get(0).getBagElements(), multiInputist
								.get(1).getBagElements());

		for (String var : intersectionVariables) {
			for (BagInformation bag : multiInputist) {
				if (bag.isVariableOptional(var)) {
					throw new RuntimeException(
							"Join over optional variable is not allowed in pig!");
				}
			}
		}

		if (debug) {
			result.append("-- Optional Join over " + intersectionVariables
					+ "\n");
		}
		result.append(newBag.getName() + " = JOIN ");

		result.append(multiInputist.get(0).getName().toString() + " BY ");

		result.append(this.getJoinElements(intersectionVariables,
				multiInputist.get(0)));

		result.append(" LEFT OUTER, "
				+ multiInputist.get(1).getName().toString() + " BY ");

		result.append(this.getJoinElements(intersectionVariables,
				multiInputist.get(1)));

		result.append(";\n\n");

		newBag.setJoinElements(multiInputist.get(0).getBagElements());

		for (String elem : multiInputist.get(1).getBagElements()) {
			newBag.addBagElements(elem);
			newBag.addOptionalElements(elem);
		}

		return result.toString();
	}

	/**
	 * Gets the intersection variables.
	 * 
	 * @param set1
	 *            the set1
	 * @param set2
	 *            the set2
	 * @return the intersection variables
	 */
	private ArrayList<String> getIntersectionVariables(ArrayList<String> set1,
			ArrayList<String> set2) {
		HashSet<String> result = new HashSet<String>();
		for (String elem1 : set1) {
			for (String elem2 : set2) {
				if (elem1.equals(elem2) && elem2.equals(elem1)) {
					result.add(elem1);
				}
			}
		}
		return new ArrayList<String>(result);
	}

	/**
	 * Gets the join elements.
	 * 
	 * @param intersectionVars
	 *            the intersection vars
	 * @param bag
	 *            the bag
	 * @return the join elements
	 */
	public String getJoinElements(ArrayList<String> intersectionVars,
			BagInformation bag) {
		StringBuilder result = new StringBuilder();
		if (intersectionVars.size() == 1) {
			result.append("$"
					+ bag.getBagElements().indexOf(intersectionVars.get(0)));
		} else {
			result.append("(");
			int i = 0;
			for (String var : intersectionVars) {
				if (i > 0) {
					result.append(",");
				}
				result.append("$" + bag.getBagElements().indexOf(var));
				i++;
			}
			result.append(")");
		}
		return result.toString();
	}

	/**
	 * Gets the variables.
	 * 
	 * @param vars
	 *            the vars
	 * @return the variables
	 */
	public String getVariables(Collection<Variable> vars) {
		StringBuilder result = new StringBuilder();
		for (Variable var : vars) {
			result.append("?" + var.getName() + " ");
		}
		return result.toString();
	}
}
