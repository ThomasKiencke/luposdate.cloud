package lupos.cloud.pig.operator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.SortedSet;
import java.util.TreeSet;

import lupos.cloud.hbase.HBaseDistributionStrategy;
import lupos.cloud.pig.JoinInformation;
import lupos.datastructures.items.Variable;
import lupos.engine.operators.tripleoperator.TriplePattern;

/**
 * Mit Hilfe dieser Klasse wird der IndexScanOperator in ein PigLatin Programm
 * übersetzt. Dazu werden die einzelnen triplePattern des Operators übergeben.
 */
public class IndexScanToPigQuery {

	/** The join variables. */
	SortedSet<String> joinVariables = new TreeSet<String>();

	/** The intermediate joins. */
	ArrayList<JoinInformation> intermediateJoins = new ArrayList<JoinInformation>();

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

		/**
		 * Für Triplepattern ?s ?p ?o wird eine beliebige Tabelle komplett
		 * geladen und alle Informationen zuürck gegeben.
		 */
		if (curPattern.allElementsAreVariables()) {
			result.append(curPattern.getName()
					+ "_DATA = "
					+ "load 'hbase://"
					+ curPattern.getName()
					+ "' "
					+ "using org.apache.pig.backend.hadoop.hbase.HBaseStorage('"
					+ HBaseDistributionStrategy.getTableInstance()
							.getColumnFamilyName()
					+ "', '-loadKey true') as (rowkey:chararray, columncontent:map[]);"
					+ "\n");

			result.append("INTERMEDIATE_BAG_"
					+ curPattern.getPatternId()
					+ " = foreach "
					+ curPattern.getName()
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
					+ curPattern.getName()
					+ "' "
					+ "using lupos.cloud.pig.udfs.HBaseLoadUDF('"
					+ HBaseDistributionStrategy.getTableInstance()
							.getColumnFamilyName() + "', '','"
					+ curPattern.getLiterals() + "') as (columncontent:map[]);"
					+ "\n");

			result.append("INTERMEDIATE_BAG_"
					+ curPattern.getPatternId()
					+ " = foreach PATTERN_"
					+ curPattern.getPatternId()
					+ " generate flatten(lupos.cloud.pig.udfs.MapToBagUDF($0)) as "
					+ ((curPattern.getJoinElements().size() == 1) ? "(output:chararray);"
							: "(output1:chararray, output2:chararray); ")
					+ "\n");
		}
		intermediateJoins.add(curPattern);
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
			ArrayList<JoinInformation> joinAliases = null;
			ArrayList<ArrayList<JoinInformation>> joinCandidates = new ArrayList<ArrayList<JoinInformation>>();
			ArrayList<String> joinVariablesCandidates = new ArrayList<String>();

			/*
			 * Es wird die Join-Menge gesucht bei dem eine Variable am
			 * häufigsten vorkommt. Für die Join-Mengen wird dann ein PigLatin
			 * Join ausgegeben und die Join-Mengen werden zu einer vereinigt.
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
			this.joinVariables.remove(variableToJoin);
		}

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
			// werden, hier wird S_PO genommen
			result = new JoinInformation(triplePattern, "S_PO");
			break;
		case 0:
			// Wenn alles Literale sind kann eine beliebige Tabelle verwendet
			// werden, hier wird SO_P genommen
			result = new JoinInformation(triplePattern, "SO_P");
			break;
		default:
			break;
		}

		for (String item : result.getVariables()) {
			joinVariables.add(item);
		}

		return result;
	}

	/**
	 * Optimiert die Ausgabe, doppelte Variablen werden nur einmal ausgegeben.
	 * 
	 * @return the string
	 */
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
		return intermediateJoins.get(0).getJoinElements();
	}
}
