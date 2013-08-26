package lupos.cloud.pig.operator;

import java.util.ArrayList;
import java.util.HashMap;

import lupos.cloud.pig.JoinInformation;
import lupos.cloud.pig.PigQuery;
import lupos.engine.operators.singleinput.filter.Filter;
import lupos.sparql1_1.*;

public class PigFilterOperator implements IPigOperator {
	private ArrayList<JoinInformation> intermediateJoins;
	Filter filter;

	public static Class[] supportedOperations = { ASTLessThanNode.class,
			ASTNotNode.class, ASTRDFLiteral.class, ASTStringLiteral.class,
			ASTGreaterThanNode.class, ASTLessThanEqualsNode.class,
			ASTGreaterThanEqualsNode.class, ASTEqualsNode.class,
			ASTNotEqualsNode.class };
	ArrayList<String> filterListe = new ArrayList<String>();
	private boolean debug;
	private ArrayList<String> variables = new ArrayList<String>();
	private String pigFilter = null;

	public PigFilterOperator(Filter filter) {
		this.filter = filter;
		pigFilter = generateFilterList(filter.getNodePointer().getChildren()[0]);
	}

	private String checkIfFilterPossible() {
		StringBuilder result = new StringBuilder();
		// Wenn alle Variablen in einer Menge vorkommen, kann der Filter
		// angewandt weden
		for (JoinInformation curJoin : intermediateJoins) {

			// Wenn die Menge nicht schon einmal gefiltert wurde
			if (!curJoin.filterApplied(this)) {
				boolean varNotFound = false;
				for (String var : variables) {
					if (!curJoin.getJoinElements().contains("?" + var)) {
						varNotFound = true;
					}
				}
				if (!varNotFound) {
					if (debug) {
						result.append("-- Filter: " + pigFilter + "\n");
					}

					JoinInformation newJoin = new JoinInformation(
							"INTERMEDIATE_BAG_" + JoinInformation.idCounter);
					

					String pigFilterVarReplaced = pigFilter;
					for (String var : variables) {
						pigFilterVarReplaced = pigFilterVarReplaced.replace(var, getPigNameForVariable("?" + var, curJoin.getJoinElements()));
					}
					
					result.append(newJoin.getName() + " = FILTER "
							+ curJoin.getName() + " BY " + pigFilterVarReplaced + ";\n");

					if (debug) {
						result.append("\n");
					}

					newJoin.setPatternId(JoinInformation.idCounter);
					newJoin.setJoinElements(curJoin.getJoinElements());
					newJoin.addAppliedFilters(this);
					newJoin.addAppliedFilters(curJoin.getAppliedFilters());
					
					intermediateJoins.remove(curJoin);
					intermediateJoins.add(newJoin);
					JoinInformation.idCounter++;
				}
			}
		}
		return result.toString();
	}

	private String generateFilterList(Node node) {
		StringBuilder result = new StringBuilder();

		/** Blätter */
		if (node.getChildren() == null) {
			filterListe.add(node.toString());
			if (node instanceof ASTVar) {
				ASTVar var = (ASTVar) node;
				result.append((var.getName()));
				variables.add(var.getName());
			} else if (node instanceof ASTStringLiteral) {
				ASTStringLiteral literal = (ASTStringLiteral) node;
				result.append("'"
						+ literal.getStringLiteral().replace("\"", "") + "'");
			} else {
				System.out.println("Not supported leaf Type: "
						+ node.getClass().getSimpleName());
				// result.append("(No Support: " + node.toString() + ")" );
			}

			/** Innere Knoten */
		} else {
			boolean closeBracket = false;
			// z.B. !A
			if (node.getChildren().length == 1) {
				filterListe.add(node.toString());
				if (node instanceof ASTNotNode) {
					// ASTNotNode notNode = (ASTNotNode) node;
					result.append(" NOT(");
					closeBracket = true;
				} else if (node instanceof ASTRDFLiteral) {
					// ignore, weil StringLiteral verarbeitet wird
				} else {
					System.out.println("Not supported inner Type 1: "
							+ node.getClass().getSimpleName());
					// result.append("(No Support for: " + node.toString() + ")"
					// );
				}
				for (Node child : node.getChildren()) {
					result.append(generateFilterList(child));
				}

				if (closeBracket) {
					closeBracket = false;
					result.append(") ");
				}
			} else
			// z.B. A = B, A != B usw.
			{
				// linker Baum
				result.append(generateFilterList(node.getChildren()[0]));
				filterListe.add(node.toString());
				if (node instanceof ASTLessThanNode) {
					result.append(" < ");
				} else if (node instanceof ASTLessThanEqualsNode) {
					result.append(" <= ");
				} else if (node instanceof ASTRegexFuncNode) {
					// TODO: SPARQL REGEX und Java Regex unterscheiden sich,
					// muss also noch angepasst werden
					result.append(" MATCHES ");
				} else if (node instanceof ASTGreaterThanNode) {
					result.append(" > ");
				} else if (node instanceof ASTGreaterThanEqualsNode) {
					result.append(" >= ");
				} else if (node instanceof ASTEqualsNode) {
					result.append(" == ");
				} else if (node instanceof ASTNotEqualsNode) {
					result.append(" != ");
				} else {
					System.out.println("Not supported inner Type 2:  "
							+ node.getClass().getSimpleName());
					// result.append("(No Support for: " + node.toString() +
					// ")");
				}
				// rechter Baum
				result.append(generateFilterList(node.getChildren()[1]));
			}
		}

		return result.toString();
	}

	public String buildQuery(PigQuery pigQuery) {
		StringBuilder result = new StringBuilder();
		this.debug = pigQuery.isDebug();
		this.intermediateJoins = pigQuery.getIntermediateJoins();
		result.append(this.checkIfFilterPossible());
		return result.toString();
	}

	private String getPigNameForVariable(String name, ArrayList<String> sparqlVariableList) {
		for (int i = 0; i < sparqlVariableList.size(); i++) {
			if (sparqlVariableList.get(i).equals(name)) {
				return "$" + i;
			}
		}
		return null; // Fall sollte nicht vorkommen
	}

}