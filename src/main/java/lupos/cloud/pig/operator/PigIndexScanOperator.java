package lupos.cloud.pig.operator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

import lupos.cloud.hbase.HBaseDistributionStrategy;
import lupos.cloud.pig.JoinInformation;
import lupos.cloud.pig.SinglePigQuery;
import lupos.datastructures.items.Variable;
import lupos.engine.operators.tripleoperator.TriplePattern;

public class PigIndexScanOperator implements IPigOperator {
	ArrayList<JoinInformation> intermediateJoins = null;
	Collection<TriplePattern> triplePatternCollection = null;
	int tripleCounter = 0;
	boolean debug = false;

	public PigIndexScanOperator(Collection<TriplePattern> tp) {
		this.triplePatternCollection = tp;
	}

	/**
	 * Mit dieser Methode wird das PigLatin-Programm langsam aufgebaut indem die
	 * einzelnen Tripel-Muster hinzuzgefügt werden.
	 * 
	 * @param triplePattern
	 *            the triple pattern
	 * @return the string
	 */
	public String buildQuery(ArrayList<JoinInformation> intermediateBags,
			boolean debug, ArrayList<PigFilterOperator> filterOps) {
		this.intermediateJoins = intermediateBags;
		this.debug = debug;
		StringBuilder result = new StringBuilder();
		for (TriplePattern triplePattern : this.triplePatternCollection) {
			JoinInformation curPattern = getHBaseTable(triplePattern);

			if (debug) {
				result.append("-- TriplePattern: " + triplePattern.toN3String()
						+ "\n");
			}
			/**
			 * Für Triplepattern ?s ?p ?o wird eine beliebige Tabelle komplett
			 * geladen und alle Informationen zuürck gegeben.
			 */
			if (curPattern.allElementsAreVariables()) {
				result.append(curPattern.getTablename()
						+ "_DATA = "
						+ "load 'hbase://"
						+ curPattern.getTablename()
						+ "' "
						+ "using org.apache.pig.backend.hadoop.hbase.HBaseStorage('"
						+ HBaseDistributionStrategy.getTableInstance()
								.getColumnFamilyName()
						+ "', '-loadKey true -caching 500') as (rowkey_" + tripleCounter
						+ ":chararray, columncontent_" + tripleCounter
						+ ":map[]);" + "\n");

				result.append(curPattern.getName()
						+ " = foreach "
						+ curPattern.getTablename()
						+ "_DATA generate $0, flatten(lupos.cloud.pig.udfs.MapToBagUDF($1));\n");
			} else if (curPattern.allElementsAreLiterals()) {
				// do nothing, maybe add in future
				return "";
			} else {
				result.append(
				/**
				 * Für alle anderen Triplepattern wird in den jeweiligen
				 * Tabellen gesucht und nur das Ergebniss (der Spaltenname)
				 * zurückgegeben.
				 */
				"PATTERN_"
						+ curPattern.getPatternId()
						+ " = "
						+ "load 'hbase://"
						+ curPattern.getTablename()
						+ "' "
						+ "using lupos.cloud.pig.udfs.HBaseLoadUDF('"
						+ HBaseDistributionStrategy.getTableInstance()
								.getColumnFamilyName() + "', '-caching 500','"
						+ curPattern.getLiterals() + "') as (columncontent_"
						+ tripleCounter + ":map[]);" + "\n");

				result.append(curPattern.getName()
						+ " = foreach PATTERN_"
						+ curPattern.getPatternId()
						+ " generate flatten(lupos.cloud.pig.udfs.MapToBagUDF($0)) as "
						+ ((curPattern.getJoinElements().size() == 1) ? "(output"
								+ tripleCounter + ":chararray);"
								: "(output1_" + tripleCounter
										+ ":chararray, output2_"
										+ tripleCounter + ":chararray); ")
						+ "\n");
			}
			intermediateJoins.add(curPattern);

			if (debug) {
				result.append("\n");
			}
			tripleCounter++;
		}
		return result.toString();
	}

	/**
	 * Gibt für ein Tripel-Muster die korrespondierende HBase Tabelle zurück.
	 * 
	 * @param triplePattern
	 *            the triple pattern
	 * @return the h base table
	 */
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
			result = new JoinInformation(triplePattern, "S_PO",
					"INTERMEDIATE_BAG_");
			break;
		case 101:
			result = new JoinInformation(triplePattern, "P_SO",
					"INTERMEDIATE_BAG_");
			break;
		case 11:
			result = new JoinInformation(triplePattern, "O_SP",
					"INTERMEDIATE_BAG_");
			break;
		case 100:
			result = new JoinInformation(triplePattern, "PS_O",
					"INTERMEDIATE_BAG_");
			break;
		case 10:
			result = new JoinInformation(triplePattern, "SO_P",
					"INTERMEDIATE_BAG_");
			break;
		case 1:
			result = new JoinInformation(triplePattern, "PO_S",
					"INTERMEDIATE_BAG_");
			break;
		case 111:
			// Wenn alles Variablen sind kann eine beliebige Tabelle verwendet
			// werden, hier wird S_PO genommen
			result = new JoinInformation(triplePattern, "S_PO",
					"INTERMEDIATE_BAG_");
			break;
		case 0:
			// Wenn alles Literale sind kann eine beliebige Tabelle verwendet
			// werden, hier wird SO_P genommen
			result = new JoinInformation(triplePattern, "SO_P",
					"INTERMEDIATE_BAG_");
			break;
		default:
			break;
		}

		// for (String item : result.getVariables()) {
		// joinVariables.add(item);
		// }

		return result;
	}

	public String multiJoinOverTwoVariables() {
		StringBuilder result = new StringBuilder();
		HashSet<String> equalVariables = null;
		HashSet<JoinInformation> toJoin = new HashSet<JoinInformation>();
		boolean found = false;

		/*
		 * Es wird die Join-Menge gesucht bei dem eine Variable am häufigsten
		 * vorkommt. Für die Join-Mengen wird dann ein PigLatin Join ausgegeben
		 * und die Join-Mengen werden zu einer vereinigt.
		 */
		for (JoinInformation curJoin1 : intermediateJoins) {
			HashSet<String> variables1 = new HashSet<String>();

			// alle Mengen die weniger als 2 variablen haben sind für diesen
			// Fall nicht interessant
			if (curJoin1.getVariables().size() < 2 || toJoin.contains(curJoin1)) {
				continue;
			}

			// Füge alle Variablen dem Set hinzu
			for (String var : curJoin1.getVariables()) {
				variables1.add(var);
			}

			// Finde eine Menge die die selben Variablen hat
			for (JoinInformation curJoin2 : intermediateJoins) {
				HashSet<String> variables2 = new HashSet<String>();

				// Die neue Menge darf nicht die selbe seine wie die erste und
				// muss auch mehr als eine Variable haben
				if (curJoin1.equals(curJoin2)
						|| curJoin2.getVariables().size() < 2
						|| toJoin.contains(curJoin2)) {
					continue;
				}

				// Füge alle Variablen dem Set hinzu
				for (String var : curJoin2.getVariables()) {
					variables2.add(var);
				}

				// Vergleiche Set1 mit Set2 und speicher selbe Variablen ab
				HashSet<String> tmpEqualVariables = new HashSet<String>();
				for (String entry1 : variables1) {
					for (String entry2 : variables2) {
						if (entry1.equals(entry2)) {
							tmpEqualVariables.add(entry1);
						}
					}
				}
				if (tmpEqualVariables.size() > 1) {
					equalVariables = tmpEqualVariables;
					found = true;
					toJoin.add(curJoin1);
					toJoin.add(curJoin2);
				}

			}

		}

		if (!found) {
			return null;
		}
		result.append(getPigMultiJoinWith2Columns(
				new ArrayList<JoinInformation>(toJoin), new ArrayList<String>(
						equalVariables)));

		for (JoinInformation toRemove : toJoin) {
			intermediateJoins.remove(toRemove);
		}
		// this.joinVariables.remove(variableToJoin);
		return result.toString();
	}

	public String multiJoinOverOneVariable() {
		StringBuilder result = new StringBuilder();
		ArrayList<JoinInformation> joinAliases = null;
		ArrayList<ArrayList<JoinInformation>> joinCandidates = new ArrayList<ArrayList<JoinInformation>>();
		ArrayList<String> joinVariablesCandidates = new ArrayList<String>();

		/*
		 * Es wird die Join-Menge gesucht bei dem eine Variable am häufigsten
		 * vorkommt. Für die Join-Mengen wird dann ein PigLatin Join ausgegeben
		 * und die Join-Mengen werden zu einer vereinigt.
		 */
		for (JoinInformation curJoin : intermediateJoins) {
			boolean found = false;
			String joinVariable = "";
			joinAliases = new ArrayList<JoinInformation>();
			// JoinInformation curJoin = intermediateJoins.get(0);
			joinAliases.add(curJoin);
			for (int i = 0; i < intermediateJoins.size(); i++) {
				if (intermediateJoins.get(i).equals(curJoin)) {
					continue;
				}
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

			joinCandidates.add(joinAliases);
			joinVariablesCandidates.add(joinVariable);

		}

		ArrayList<JoinInformation> patternToJoin = joinCandidates.get(0);
		String variableToJoin = joinVariablesCandidates.get(0);
		int i = 0;
		for (ArrayList<JoinInformation> curCandidate : joinCandidates) {
			if (curCandidate.size() > patternToJoin.size()) {
				patternToJoin = curCandidate;
				variableToJoin = joinVariablesCandidates.get(i);
			}
			i++;
		}

		result.append(getPigMultiJoin(patternToJoin, variableToJoin));

		for (JoinInformation toRemove : patternToJoin) {
			intermediateJoins.remove(toRemove);
		}
		// this.joinVariables.remove(variableToJoin);
		return result.toString();
	}

	/**
	 * Joint mehrere Mengen über ein Element (Ausgabe als PigLatin Programm).
	 * 
	 * @param joinOverItem
	 *            the join over item
	 * @param joinElement
	 *            the join element
	 * @return the pig multi join
	 */
	public String getPigMultiJoin(ArrayList<JoinInformation> joinOverItem,
			String joinElement) {
		StringBuilder result = new StringBuilder();

		for (JoinInformation bag : joinOverItem) {
			if (bag.isVariableOptional(joinElement)) {
				throw new RuntimeException(
						"Join over optional variable is not allowed in pig!");
			}
		}

		if (debug) {
			result.append("-- Join over "+ joinElement.toString() +"\n");
		}


		JoinInformation curJoinInfo = new JoinInformation("INTERMEDIATE_BAG_"
				+ JoinInformation.idCounter);
		result.append(curJoinInfo.getName() + " = JOIN");
		int i = 0;
		for (JoinInformation curPattern : joinOverItem) {
			i++;
			for (String s : curPattern.getJoinElements()) {
				curJoinInfo.getJoinElements().add(s);
			}
			result.append(" " + curPattern.getName() + " BY $"
					+ curPattern.getItemPos(joinElement));
			if (i < joinOverItem.size()) {
				result.append(",");
			} else {
				result.append(";\n");
			}

			for (String elem : curPattern.getOptionalJoinElements()) {
				curJoinInfo.addOptionalElements(elem);
			}
		}
		curJoinInfo.setPatternId(JoinInformation.idCounter);
		curJoinInfo.addAppliedFilters(JoinInformation
				.mergeAppliedFilters(joinOverItem));
		intermediateJoins.add(curJoinInfo);
		JoinInformation.idCounter++;

		return result.toString();
	}

	public String getPigMultiJoinWith2Columns(
			ArrayList<JoinInformation> joinOverItem,
			ArrayList<String> joinElements) {
		StringBuilder result = new StringBuilder();

		for (String var : joinElements) {
			for (JoinInformation bag : joinOverItem) {
				if (bag.isVariableOptional(var)) {
					throw new RuntimeException(
							"Join over optional variable is not allowed in pig!");
				}
			}
		}

		if (debug) {
			result.append("-- Join over "+ joinElements.toString() +"\n");
		}
		
		JoinInformation curJoinInfo = new JoinInformation("INTERMEDIATE_BAG_"
				+ JoinInformation.idCounter);
		
		result.append(curJoinInfo.getName() + " = JOIN");
		int i = 0;
		for (JoinInformation curPattern : joinOverItem) {
			i++;
			for (String s : curPattern.getJoinElements()) {
				curJoinInfo.getJoinElements().add(s);
			}
			result.append(" " + curPattern.getName() + " BY ($"
					+ curPattern.getItemPos(joinElements.get(0)) + ",$"
					+ curPattern.getItemPos(joinElements.get(1)) + ")");
			if (i < joinOverItem.size()) {
				result.append(",");
			} else {
				result.append(";\n");
			}

			for (String elem : curPattern.getOptionalJoinElements()) {
				curJoinInfo.addOptionalElements(elem);
			}
		}
		curJoinInfo.setPatternId(JoinInformation.idCounter);
		curJoinInfo.addAppliedFilters(JoinInformation
				.mergeAppliedFilters(joinOverItem));

		intermediateJoins.add(curJoinInfo);
		JoinInformation.idCounter++;

		return result.toString();
	}

	public String getFinalAlias() {
		return intermediateJoins.get(0).getName();
	}
}
