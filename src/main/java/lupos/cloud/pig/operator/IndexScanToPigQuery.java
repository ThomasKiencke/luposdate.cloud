package lupos.cloud.pig.operator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import lupos.cloud.hbase.HBaseDistributionStrategy;
import lupos.cloud.pig.JoinInformation;
import lupos.datastructures.items.Variable;
import lupos.engine.operators.singleinput.Projection;
import lupos.engine.operators.tripleoperator.TriplePattern;

/**
 * Mit Hilfe dieser Klasse wird der IndexScanOperator in ein PigLatin Programm
 * übersetzt. Dazu werden die einzelnen triplePattern des Operators übergeben.
 */
public class IndexScanToPigQuery {

	/** The intermediate joins. */
	ArrayList<JoinInformation> intermediateJoins = new ArrayList<JoinInformation>();
	ArrayList<String> projectionVariables = new ArrayList<String>();
	ArrayList<FilterToPigQuery> filterList = new ArrayList<FilterToPigQuery>();
	private boolean debug = true;

	/**
	 * Mit dieser Methode wird das PigLatin-Programm langsam aufgebaut indem die
	 * einzelnen Tripel-Muster hinzuzgefügt werden.
	 * 
	 * @param triplePattern
	 *            the triple pattern
	 * @return the string
	 */
	public String buildQuery(TriplePattern triplePattern) {
		StringBuilder result = new StringBuilder();
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
					+ "', '-loadKey true') as (rowkey:chararray, columncontent:map[]);"
					+ "\n");

			result.append(curPattern.getName()
					+ " = foreach "
					+ curPattern.getTablename()
					+ "_DATA generate $0, flatten(lupos.cloud.pig.udfs.MapToBagUDF($1));\n");
		} else if (curPattern.allElementsAreLiterals()) {
			// do nothing
			return "";
		} else {
			result.append(
			/**
			 * Für alle anderen Triplepattern wird in den jeweiligen Tabellen
			 * gesucht und nur das Ergebniss (der Spaltenname) zurückgegeben.
			 */
			"PATTERN_"
					+ curPattern.getPatternId()
					+ " = "
					+ "load 'hbase://"
					+ curPattern.getTablename()
					+ "' "
					+ "using lupos.cloud.pig.udfs.HBaseLoadUDF('"
					+ HBaseDistributionStrategy.getTableInstance()
							.getColumnFamilyName() + "', '','"
					+ curPattern.getLiterals() + "') as (columncontent:map[]);"
					+ "\n");

			result.append(curPattern.getName()
					+ " = foreach PATTERN_"
					+ curPattern.getPatternId()
					+ " generate flatten(lupos.cloud.pig.udfs.MapToBagUDF($0)) as "
					+ ((curPattern.getJoinElements().size() == 1) ? "(output:chararray);"
							: "(output1:chararray, output2:chararray); ")
					+ "\n");
		}
		intermediateJoins.add(curPattern);
		
		if (debug) {
			result.append("\n");
		}
		
		return result.toString();
	}

	/**
	 * Gibt den JOIN-PigLatin Programm zurück. Es wird dabei ein Multi-Join
	 * durchgeführt.
	 * 
	 * @return the join query
	 */
	public String getJoinQuery() {
		String result = multiJoin();

		return result;
	}

	public ArrayList<JoinInformation> getIntermediateJoins() {
		return intermediateJoins;
	}

	/**
	 * Multi join über alle Tripel-Muster. Dabei wird zuerst über die Variable
	 * gejoint die in den meisten Tripel-Pattern vorkommt usw.
	 * 
	 * @return the string
	 */
	private String multiJoin() {
		StringBuilder result = new StringBuilder();
		// suche so lange bis es noch Mengen zum joinen gibt
		while (intermediateJoins.size() > 1) {
			/*
			 * Überprüfe bei jeden durchlauf ob eine Projektion durchgeführt
			 * werden kann (Grund: Projektion so früh wie möglich)
			 */
			result.append(checkIfProjectionPossible());
			// System.out.println("size: " + intermediateJoins.size());
			String multiJoinOverTwoVars = this.multiJoinOverTwoVariablse();

			/*
			 * Es werden immer erst Tripel-Muster gesucht bei denen über zwei
			 * Variablen gejoint werden kann und erst dann die Muster wo über
			 * eine Variable gejoint wird. Beispiel: {?s ?p ?o . <literal> ?p
			 * ?o}
			 */
			
			if (debug) {
				result.append("-- Join \n");
			}
			if (multiJoinOverTwoVars != null) {
				result.append(multiJoinOverTwoVars);
			} else {
				result.append(multiJoinOverOneVariable());
			}
			
			if (debug) {
				result.append("\n");
			}
		}
		
		// Eine Projektion zum Schluss
		result.append(checkIfProjectionPossible());
		return result.toString();
	}

	private String checkIfProjectionPossible() {
		StringBuilder result = new StringBuilder();
		if (projectionVariables.size() != 0) {
			HashMap<JoinInformation, ArrayList<String>> varJoinMap = getValidProjectionVariables();

			for (JoinInformation curJoin : varJoinMap.keySet()) {

				// Projektion ist nicht notwendig
				if (joinListAndProjectionListAreEquals(
						curJoin.getJoinElements(), varJoinMap.get(curJoin))) {
					continue;
				} else {
					if (debug) {
						result.append("-- Projection: "
								+ varJoinMap.get(curJoin).toString()
										.replace("[", "").replace("]", "")
								+ "\n");
					}
					System.out.println("curJoinSize: "
							+ curJoin.getJoinElements().size() + " andere: "
							+ varJoinMap.get(curJoin));
					result.append("INTERMEDIATE_BAG_"
							+ JoinInformation.idCounter + " = FOREACH "
							+ curJoin.getName() + " GENERATE ");
					JoinInformation newJoin = new JoinInformation(
							"INTERMEDIATE_BAG_" + JoinInformation.idCounter);
					int i = 0;
					for (String var : varJoinMap.get(curJoin)) {
						newJoin.getJoinElements().add(var);
						result.append("$"
								+ curJoin.getJoinElements().indexOf(var));
						if (i + 1 < varJoinMap.get(curJoin).size()) {
							result.append(", ");
						} else {
							result.append(";\n");
						}
						i++;
					}
					
					if (debug) {
						result.append("\n");
					}
					
					newJoin.setPatternId(JoinInformation.idCounter);
					newJoin.setJoinElements(varJoinMap.get(curJoin));
					intermediateJoins.remove(curJoin);
					intermediateJoins.add(newJoin);
					JoinInformation.idCounter++;
				}
			}
		}
		return result.toString();
	}
	
	private HashMap<JoinInformation, ArrayList<String>> getValidProjectionVariables() {
		HashMap<JoinInformation, ArrayList<String>> varJoinMap = new HashMap<JoinInformation, ArrayList<String>>();
			for (String projectionVar : projectionVariables) {
				int varCounter = 0;
				JoinInformation projectionJoin = null;
				for (JoinInformation item : intermediateJoins) {
					if (item.getJoinElements().contains(projectionVar)) {
						varCounter++;
						projectionJoin = item;
					}
				}

				if (varCounter == 1) {
					// prüfe ob gedropte variablen noch gebraucht werden,
					// aonsonsten nicht droppen
					ArrayList<String> dropNoteAllowedList = new ArrayList<String>();
					for (String dropCandidateVariable : projectionJoin
							.getJoinElements()) {
						for (JoinInformation otherJoin : intermediateJoins) {
							if (!otherJoin.equals(projectionJoin)) {
								if (otherJoin.getJoinElements().contains(
										dropCandidateVariable)) {
									dropNoteAllowedList
											.add(dropCandidateVariable);
								}

							}
						}
					}

					// Wende die Projetion nur an, wenn sich die Liste
					// verkleinert
					if (dropNoteAllowedList.size() + 1 < projectionJoin
							.getJoinElements().size()) {

						ArrayList<String> varList = varJoinMap
								.get(projectionJoin);
						if (varList != null) {
							varList.add(projectionVar);
						} else {
							ArrayList<String> newList = new ArrayList<String>();
							newList.add(projectionVar);
							varJoinMap.put(projectionJoin, newList);
						}

						for (String noDrop : dropNoteAllowedList) {
							varJoinMap.get(projectionJoin).add(noDrop);
						}
					}
				}
			}
			return varJoinMap;
	}

	private boolean joinListAndProjectionListAreEquals(
			ArrayList<String> joinElements, ArrayList<String> compareList) {
		for (String elem : joinElements) {
			if (!compareList.contains(elem)) {
				return false;
			}
		}
		return true;
	}

	private String multiJoinOverTwoVariablse() {
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
		result.append(getPigMultiJoin(new ArrayList<JoinInformation>(toJoin),
				new ArrayList<String>(equalVariables)));

		for (JoinInformation toRemove : toJoin) {
			intermediateJoins.remove(toRemove);
		}
		// this.joinVariables.remove(variableToJoin);
		return result.toString();
	}

	private String multiJoinOverOneVariable() {
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
		}
		curJoinInfo.setPatternId(JoinInformation.idCounter);
		intermediateJoins.add(curJoinInfo);
		JoinInformation.idCounter++;

		return result.toString();
	}

	public String getPigMultiJoin(ArrayList<JoinInformation> joinOverItem,
			ArrayList<String> joinElements) {
		StringBuilder result = new StringBuilder();
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
		}
		curJoinInfo.setPatternId(JoinInformation.idCounter);
		intermediateJoins.add(curJoinInfo);
		JoinInformation.idCounter++;

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

	/**
	 * Optimiert die Ausgabe, doppelte Variablen werden nur einmal ausgegeben.
	 * 
	 * @deprecated  Die Projektionsmethode übernimmt diese Aufgabe jetzt.
	 * @return the string
	 */
	@Deprecated
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

		// alias hass to be added "outsite" of the method
		result.append(" = FOREACH " + "INTERMEDIATE_BAG_"
				+ intermediateJoins.get(0).getPatternId() + " GENERATE " + list
				+ ";");

		return result.toString();
	}

	/**
	 * Gibt die Variablenreihenfolge zurück.
	 * 
	 * @return the result order
	 */
	public ArrayList<String> getResultOrder() {
		ArrayList<String> result = new ArrayList<String>();
		for (String elem : intermediateJoins.get(0).getJoinElements()) {
			result.add(elem.replace("?", ""));
		}
		return result;
	}

	public void setProjection(Projection projection) {
		this.projectionVariables = new ArrayList<String>();
		for (Variable varToAdd : projection.getProjectedVariables()) {
			this.projectionVariables.add(varToAdd.toString());
		}
	}
	
	public String getFinalAlias() {
		return intermediateJoins.get(0).getName();
	}
}
