package lupos.cloud.pig.operator;

import java.util.ArrayList;

import lupos.cloud.pig.JoinInformation;
import lupos.cloud.pig.SinglePigQuery;
import lupos.datastructures.items.Variable;
import lupos.engine.operators.multiinput.join.Join;

public class PigJoinOperator implements IPigOperator {
	private boolean debug;
	private JoinInformation newJoin;
	private ArrayList<JoinInformation> multiInputist;
	private Join join;

	public PigJoinOperator(JoinInformation newJoin,
			ArrayList<JoinInformation> multiInputist, Join join) {
		this.newJoin = newJoin;
		this.multiInputist = multiInputist;
		this.join = join;
	}

	public String buildQuery(ArrayList<JoinInformation> intermediateBags, boolean debug, ArrayList<PigFilterOperator> filterOps) {
		this.debug = debug;
		StringBuilder result = new StringBuilder();
		if (debug) {
			result.append(" -- JOIN:\n");
		}
		result.append(newJoin.getName() + " = JOIN ");
		for (int i = 0; i < multiInputist.size(); i++) {
			if (i > 0) {
				result.append(", ");
			}

			result.append(multiInputist.get(i).getName() + " BY ");

			ArrayList<Variable> joinList = new ArrayList<Variable>(
					join.getIntersectionVariables());
			if (joinList.size() == 1) {
				result.append("$"
						+ multiInputist.get(i).getJoinElements()
								.indexOf("?" + joinList.get(0).getName()));
			} else {
				i = 0;
				result.append("(");
				for (Variable var : joinList) {
					if (i > 0) {
						result.append(",");
					}
					result.append("$"
							+ multiInputist.get(i).getJoinElements()
									.indexOf("?" + var.getName()));
					i++;
				}
				result.append(")");
			}
		}
		result.append(";\n\n");
		return result.toString();
	}
}
