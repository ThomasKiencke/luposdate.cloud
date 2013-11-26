package lupos.cloud.pig.operator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

import lupos.cloud.pig.BagInformation;
import lupos.cloud.pig.SinglePigQuery;
import lupos.datastructures.items.Variable;
import lupos.engine.operators.multiinput.join.Join;
import lupos.engine.operators.multiinput.optional.Optional;

public class PigOptionalOperator implements IPigOperator {
	private boolean debug;
	private BagInformation newBag;
	private ArrayList<BagInformation> multiInputist;
	private Optional optionalJoin;

	public PigOptionalOperator(BagInformation newBag,
			ArrayList<BagInformation> multiInputist, Optional optional) {
		this.newBag = newBag;
		this.multiInputist = multiInputist;
		this.optionalJoin = optional;
	}

	public String buildQuery(ArrayList<BagInformation> intermediateBags,
			boolean debug, ArrayList<PigFilterOperator> filterOps) {
		this.debug = debug;
		StringBuilder result = new StringBuilder();

		ArrayList<String> intersectionVariables = this.getIntersectionVariables(
				multiInputist.get(0).getJoinElements(), multiInputist.get(1)
						.getJoinElements());
		
		for (String var : intersectionVariables) {
			for (BagInformation bag : multiInputist) {
				if (bag.isVariableOptional(var)) {
					throw new RuntimeException(
							"Join over optional variable is not allowed in pig!");
				}
			}
		}

		if (debug) {
			result.append("-- Optional Join over "
					+ intersectionVariables
					+ "\n");
		}
		result.append(newBag.getName() + " = JOIN ");

		result.append(multiInputist.get(0).getName().toString() + " BY ");

		result.append(this.getJoinElements(
				intersectionVariables,
				multiInputist.get(0)));

		result.append(" LEFT OUTER, "
				+ multiInputist.get(1).getName().toString() + " BY ");

		result.append(this.getJoinElements(
				intersectionVariables,
				multiInputist.get(1)));

		result.append(";\n\n");

		newBag.setJoinElements(multiInputist.get(0).getJoinElements());

		for (String elem : multiInputist.get(1).getJoinElements()) {
			// if (!multiInputist.get(0).getJoinElements().contains(elem)) {
			newBag.addBagElements(elem);
			newBag.addOptionalElements(elem);
			// }
		}

		return result.toString();
	}

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

	public String getJoinElements(ArrayList<String> intersectionVars,
			BagInformation bag) {
		StringBuilder result = new StringBuilder();
		if (intersectionVars.size() == 1) {
			result.append("$"
					+ bag.getJoinElements().indexOf(
							 intersectionVars.get(0)));
		} else {
			result.append("(");
			int i = 0;
			for (String var : intersectionVars) {
				if (i > 0) {
					result.append(",");
				}
				result.append("$"
						+ bag.getJoinElements().indexOf(var));
				i++;
			}
			result.append(")");
		}
		return result.toString();
	}

	public String getVariables(Collection<Variable> vars) {
		StringBuilder result = new StringBuilder();
		for (Variable var : vars) {
			result.append("?" + var.getName() + " ");
		}
		return result.toString();
	}
}
