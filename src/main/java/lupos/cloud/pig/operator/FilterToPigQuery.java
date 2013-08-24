package lupos.cloud.pig.operator;

import java.util.ArrayList;

import lupos.cloud.pig.JoinInformation;
import lupos.cloud.pig.PigQuery;
import lupos.engine.operators.singleinput.filter.Filter;
import lupos.sparql1_1.*;

public class FilterToPigQuery {
	StringBuilder queryBuild = new StringBuilder();
	ArrayList<String> sparqlVariableList;
	Filter filter;
	boolean closeBracket = false;

	public static Class[] supportedOperations = { ASTLessThanNode.class,
			ASTNotNode.class, ASTRDFLiteral.class, ASTStringLiteral.class, ASTGreaterThanNode.class};
	ArrayList<String> filterListe = new ArrayList<String>();

	public FilterToPigQuery(Filter filter) {
		this.filter = filter;
	}

	private String generateFilterList(Node node) {
		StringBuilder result = new StringBuilder();

		/** Blätter */
		if (node.getChildren() == null) {
			filterListe.add(node.toString());
			if (node instanceof ASTVar) {
				ASTVar var = (ASTVar) node;
				result.append(getPigNameForVariable(var.getName()));
				if (closeBracket) {
					this.closeBracket = false;
					result.append(") ");
				}
			} else if (node instanceof ASTStringLiteral) {
				ASTStringLiteral literal = (ASTStringLiteral) node;
				result.append(literal.getStringLiteral().replace("\"", "'"));
			} else {
				System.out.println("Not supported leaf Type: "
						+ node.getClass().getSimpleName());
				// result.append("(No Support: " + node.toString() + ")" );
			}

			/** Innere Knoten */
		} else {
			// z.B. !A
			if (node.getChildren().length == 1) {
				filterListe.add(node.toString());
				if (node instanceof ASTNotNode) {
					ASTNotNode notNode = (ASTNotNode) node;
					result.append(" NOT(");
					this.closeBracket = true;
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
			} else
			// z.B. A = B, A != B usw.
			{
				// linker Baum
				result.append(generateFilterList(node.getChildren()[0]));
				filterListe.add(node.toString());
				if (node instanceof ASTLessThanNode) {
					result.append(" < ");
				} else

				if (node instanceof ASTGreaterThanNode) {
					result.append(" > ");
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

	public String getPigLatinProgramm(ArrayList<String> resultOrder) {
		StringBuilder result = new StringBuilder();
		this.sparqlVariableList = resultOrder;
		result.append("X = FILTER NOFILTER BY ");
		result.append(generateFilterList(filter.getNodePointer().getChildren()[0]));
		result.append(";");
		System.out.println("Liste: " + this.filterListe.toString());
		return result.toString();
	}

	private String getPigNameForVariable(String name) {
		for (int i = 0; i < sparqlVariableList.size(); i++) {
			if (sparqlVariableList.get(i).equals(name)) {
				return "$" + i;
			}
		}
		return null; // Fall sollte nicht vorkommen
	}
}
