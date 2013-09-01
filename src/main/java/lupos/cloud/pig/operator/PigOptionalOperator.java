package lupos.cloud.pig.operator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

import lupos.cloud.pig.JoinInformation;
import lupos.cloud.pig.SinglePigQuery;
import lupos.datastructures.items.Variable;
import lupos.engine.operators.multiinput.join.Join;
import lupos.engine.operators.multiinput.optional.Optional;

public class PigOptionalOperator implements IPigOperator {
	private boolean debug;
	private JoinInformation newBag;
	private ArrayList<JoinInformation> multiInputist;
	private Optional optionalJoin;

	public PigOptionalOperator(JoinInformation newBag,
			ArrayList<JoinInformation> multiInputist, Optional optional) {
		this.newBag = newBag;
		this.multiInputist = multiInputist;
		this.optionalJoin = optional;
	}

	public String buildQuery(ArrayList<JoinInformation> intermediateBags,
			boolean debug, ArrayList<PigFilterOperator> filterOps) {
		this.debug = debug;
		StringBuilder result = new StringBuilder();

		for (Variable var : optionalJoin.getIntersectionVariables()) {
			for (JoinInformation bag : multiInputist) {
				if (bag.isVariableOptional("?" + var.getName())) {
					throw new RuntimeException(
							"Join over optional variable is not allowed in pig!");
				}
			}
		}

		if (debug) {
			result.append("-- Optional Join over "
					+ this.getVariables(optionalJoin.getIntersectionVariables())
					+ "\n");
		}
		result.append(newBag.getName() + " = JOIN ");

		result.append(multiInputist.get(0).getName().toString() + " BY ");

		result.append(this.getJoinElements(
				new ArrayList<Variable>(optionalJoin.getIntersectionVariables()),
				multiInputist.get(0)));

		result.append(" LEFT OUTER, "
				+ multiInputist.get(1).getName().toString() + " BY ");

		result.append(this.getJoinElements(
				new ArrayList<Variable>(optionalJoin.getIntersectionVariables()),
				multiInputist.get(1)));

		result.append(";\n\n");

		newBag.setJoinElements(multiInputist.get(0).getJoinElements());

		for (String elem : multiInputist.get(1).getJoinElements()) {
			if (!multiInputist.get(0).getJoinElements().contains(elem)) {
				newBag.addJoinElements(elem);
				newBag.addOptionalElements(elem);
			}
		}

		return result.toString();
	}

	public String getJoinElements(ArrayList<Variable> intersectionVars,
			JoinInformation bag) {
		StringBuilder result = new StringBuilder();
		if (intersectionVars.size() == 1) {
			result.append("$"
					+ bag.getJoinElements().indexOf(
							"?" + intersectionVars.get(0).getName()));
		} else {
			result.append("(");
			int i = 0;
			for (Variable var : intersectionVars) {
				if (i > 0) {
					result.append(",");
				}
				result.append("$"
						+ bag.getJoinElements().indexOf("?" + var.getName()));
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
