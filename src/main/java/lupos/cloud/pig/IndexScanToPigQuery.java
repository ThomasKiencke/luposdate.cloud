package lupos.cloud.pig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.SortedSet;
import java.util.TreeSet;

import org.stringtemplate.v4.compiler.CodeGenerator.includeExpr_return;

import lupos.datastructures.items.Variable;
import lupos.engine.operators.tripleoperator.TriplePattern;

public class IndexScanToPigQuery {
	SortedSet<String> joinVariables = new TreeSet<String>();
	// HashMap<String, ArrayList<String>> intermediateJoins = new
	// HashMap<String, ArrayList<String>>();
	ArrayList<JoinInformation> intermediateJoins = new ArrayList<JoinInformation>();

	public String buildQuery(TriplePattern triplePattern) {
		StringBuilder result = new StringBuilder();
		JoinInformation curPattern = getHBaseTable(triplePattern);

		result.append(curPattern.getName()
				+ "_DATA = "
				+ "load 'hbase://"
				+ curPattern.getName()
				+ "' "
				+ "using org.apache.pig.backend.hadoop.hbase.HBaseStorage('VALUE', '-loadKey true') as (rowkey:chararray, columncontent:map[]);"
				+ "\n");

		/** Für Triplepattern ?s ?p ?o */
		if (curPattern.allElementsAreVariables()) {
			result.append("INTERMEDIATE_BAG_"
					+ curPattern.getPatternId()
					+ " = foreach "
					+ curPattern.getName()
					+ "_DATA generate $0, flatten(lupos.cloud.pig.udfs.MapToBag($1));\n");
		} else {
			/** Für alle anderen Triplepattern */
			result.append("PATTERN_" + curPattern.getPatternId() + " = FILTER "
					+ curPattern.getName() + "_DATA BY $0 == '"
					+ curPattern.getLiterals() + "';" + "\n");
			result.append("INTERMEDIATE_BAG_"
					+ curPattern.getPatternId()
					+ " = foreach PATTERN_"
					+ curPattern.getPatternId()
					+ " generate flatten(lupos.cloud.pig.udfs.MapToBag($1)) as "
					+ ((curPattern.getJoinElements().size() == 1) ? "(output:chararray);"
							: "(output1:chararray, output2:chararray); ")
					+ "\n");
		}
		intermediateJoins.add(curPattern);
		return result.toString();
	}

	public String getJoinQuery() {
		String result = multiJoin();

		return result;
	}
	
	private String multiJoin() {
		StringBuilder result = new StringBuilder();

		while (intermediateJoins.size() > 1) {
			boolean found = false;
			String joinVariable = "";
			ArrayList<JoinInformation> joinAliases = new ArrayList<JoinInformation>();
			JoinInformation curJoin = intermediateJoins.get(0);
			joinAliases.add(curJoin);
			for (int i = 1; i < intermediateJoins.size(); i++) {
				for (String variable1 : curJoin.getJoinElements()) {
					if (found) {
						variable1 = joinVariable;
					}
					for (String variable2 : intermediateJoins.get(i)
							.getJoinElements()) {
						if (variable1.equals(variable2)) {
							found = true;
							joinVariable = variable1;
							joinAliases.add(intermediateJoins.get(i));
							break;
						}
					}
					if (found) {
						break;
					}
				}
			}

			result.append(getPigMultiJoin(joinAliases, joinVariable));

			for (JoinInformation toRemove : joinAliases) {
				intermediateJoins.remove(toRemove);
			}
			joinVariables.remove(joinVariable);
		}

		return result.toString();
	}

	public String getPigMultiJoin(ArrayList<JoinInformation> joinOverItem,
			String joinElement) {
		StringBuilder result = new StringBuilder();
		result.append("INTERMEDIATE_BAG_" + JoinInformation.idCounter
				+ " = JOIN");
		JoinInformation curJoinInfo = new JoinInformation("INTERMEDIATE_BAG_"
				+ JoinInformation.idCounter);
		int i = 0;
		for (JoinInformation curPattern : joinOverItem) {
			i++;
			for (String s : curPattern.getJoinElements()) {
				curJoinInfo.getJoinElements().add(s);
			}
			result.append(" INTERMEDIATE_BAG_" + curPattern.getPatternId()
					+ " BY $" + curPattern.getItemPos(joinElement));
			// curPattern.getJoinElements().remove(joinElement);
			if (i < joinOverItem.size()) {
				result.append(",");
			} else {
				result.append(";\n");
			}
		}
		curJoinInfo.setPatternId(JoinInformation.idCounter);
		intermediateJoins.add(curJoinInfo);
		JoinInformation.idCounter++;

		return result.toString();
	}

	private JoinInformation getHBaseTable(TriplePattern triplePattern) {
		int subject = triplePattern.getSubject().getClass() == Variable.class ? 1
				: 0;
		int predicate = triplePattern.getPredicate().getClass() == Variable.class ? 10
				: 0;
		int object = triplePattern.getObject().getClass() == Variable.class ? 100
				: 0;

		JoinInformation result = null;
		switch (subject + predicate + object) {
		case 110:
			result = new JoinInformation(triplePattern, "S_PO");
			break;
		case 101:
			result = new JoinInformation(triplePattern, "P_SO");
			break;
		case 11:
			result = new JoinInformation(triplePattern, "O_SP");
			break;
		case 100:
			result = new JoinInformation(triplePattern, "PS_O");
			break;
		case 10:
			result = new JoinInformation(triplePattern, "SO_P");
			break;
		case 1:
			result = new JoinInformation(triplePattern, "PO_S");
			break;
		case 111:
			// Wenn alles Variablen sind kann eine beliebige Tabelle verwendet
			// werdens
			result = new JoinInformation(triplePattern, "S_PO");
			break;
		default:
			// TODO: SPO
			break;
		}

		for (String item : result.getVariables()) {
			joinVariables.add(item);
		}

		return result;
	}

	public String optimizeResultOrder() {
		StringBuilder result = new StringBuilder();
		HashMap<String, Boolean> existMap = new HashMap<String, Boolean>();
		ArrayList<Integer> keepList = new ArrayList<Integer>();
		ArrayList<String> optimizedList = new ArrayList<String>();
		int i = 0;
		for (String element : intermediateJoins.get(0).getJoinElements()) {
			if (existMap.get(element) == null) {
				existMap.put(element, true);
				keepList.add(i);
				optimizedList.add(element.replace("?", ""));
			}
			i++;
		}

		intermediateJoins.get(0).setJoinElements(optimizedList);
		String list = "";
		boolean first = true;
		for (Integer id : keepList) {
			if (first) {
				list += "$" + id;
				first = false;
			} else
				list += ", $" + id;
		}

		result.append("X = FOREACH " + "INTERMEDIATE_BAG_"
				+ intermediateJoins.get(0).getPatternId() + " GENERATE " + list
				+ ";");
		return result.toString();
	}

	public ArrayList<String> getResultOrder() {
		return intermediateJoins.get(0).getJoinElements();
	}
}
