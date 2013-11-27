package lupos.cloud.pig.operator;

import java.util.ArrayList;
import java.util.HashSet;

import lupos.cloud.pig.BagInformation;
import lupos.engine.operators.singleinput.filter.Filter;
import lupos.sparql1_1.*;

/**
 * Filter-Operation.
 */
public class PigFilterOperator {

	/** Luposdate Filter. */
	Filter filter;

	/** Unterstütze Filter-Typen. */
	@SuppressWarnings("rawtypes")
	public static Class[] supportedOperations = { ASTVar.class,
			ASTLessThanNode.class, ASTNotNode.class, ASTRDFLiteral.class,
			ASTStringLiteral.class, ASTGreaterThanNode.class,
			ASTLessThanEqualsNode.class, ASTGreaterThanEqualsNode.class,
			ASTEqualsNode.class, ASTNotEqualsNode.class, ASTBoundFuncNode.class };

	/** Filterliste. */
	ArrayList<String> filterListe = new ArrayList<String>();

	/** Variablenliste. */
	private ArrayList<String> variables = new ArrayList<String>();

	/** PigLatin Filter. */
	private String pigFilter = null;

	/**
	 * Instantiates a new pig filter operator.
	 * 
	 * @param filter
	 *            the filter
	 */
	public PigFilterOperator(Filter filter) {
		this.filter = filter;
		pigFilter = generateFilterList(filter.getNodePointer().getChildren()[0]);
	}

	/**
	 * Gets the pig filter.
	 * 
	 * @return the pig filter
	 */
	public String getPigFilter() {
		return pigFilter;
	}

	/**
	 * Generate filter list.
	 * 
	 * @param node
	 *            the node
	 * @return the string
	 */
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
				} else if (node instanceof ASTBoundFuncNode) {
					// ASTBoundFuncNode bound = (ASTBoundFuncNode) node;
					// ASTNotNode notNode = (ASTNotNode) node;
					result.append(" lupos.cloud.pig.udfs.BoundFilterUDF(");
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

	/**
	 * Builds the query.
	 * 
	 * @param intermediateBags
	 *            the intermediate bags
	 * @param debug
	 *            the debug
	 * @param filterOps
	 *            the filter ops
	 * @return the string
	 * @deprecated Use PigFilterExecuter!
	 */
	@Deprecated
	public String buildQuery(ArrayList<BagInformation> intermediateBags,
			boolean debug, ArrayList<PigFilterOperator> filterOps) {
		return null;
	}

	/**
	 * Check if filter is supported.
	 * 
	 * @param node
	 *            the node
	 * @return true, if successful
	 */
	@SuppressWarnings("rawtypes")
	public static boolean checkIfFilterIsSupported(Node node) {
		ArrayList<Class> supportedClasses = new ArrayList<Class>();
		for (Class clazz : supportedOperations) {
			supportedClasses.add(clazz);
		}

		/** Blätter */
		if (node.getChildren() == null) {
			if (!supportedClasses.contains(node.getClass())) {
				return false;
			}

			/** Innere Knoten */
		} else {
			if (node.getChildren().length == 1) {
				if (!supportedClasses.contains(node.getClass())) {
					return false;
				}
				if (!supportedClasses
						.contains(node.getChildren()[0].getClass())) {
					return false;
				}
			} else {
				// linker Baum
				checkIfFilterIsSupported(node.getChildren()[0]);

				if (!supportedClasses.contains(node.getClass())) {
					return false;
				}

				// rechter Baum
				checkIfFilterIsSupported(node.getChildren()[1]);
			}
		}

		return true;
	}

	/**
	 * Gets the filter variables.
	 * 
	 * @param node
	 *            the node
	 * @return the filter variables
	 */
	public static HashSet<String> getFilterVariables(Node node) {
		HashSet<String> result = new HashSet<String>();

		/** Blätter */
		if (node.getChildren() == null) {
			if (node instanceof ASTVar) {
				result.add(((ASTVar) node).getName());
			}

			/** Innere Knoten */
		} else {
			if (node.getChildren().length == 1) {
				result.addAll(getFilterVariables(node.getChildren()[0]));
			} else {
				// linker Baum
				result.addAll(getFilterVariables(node.getChildren()[0]));
				// rechter Baum
				result.addAll(getFilterVariables(node.getChildren()[1]));
			}
		}

		return result;
	}

	/**
	 * Gets the variables.
	 * 
	 * @return the variables
	 */
	public ArrayList<String> getVariables() {
		return variables;
	}
}
