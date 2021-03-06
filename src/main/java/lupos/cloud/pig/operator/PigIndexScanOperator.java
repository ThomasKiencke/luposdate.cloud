package lupos.cloud.pig.operator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

import org.apache.commons.codec.digest.DigestUtils;

import lupos.cloud.bloomfilter.BitvectorManager;
import lupos.cloud.bloomfilter.CloudBitvector;
import lupos.cloud.hbase.HBaseDistributionStrategy;
import lupos.cloud.pig.BagInformation;
import lupos.cloud.storage.util.CloudManagement;
import lupos.datastructures.items.Variable;
import lupos.engine.operators.tripleoperator.TriplePattern;

/**
 * Überführt Luposdate IndexScan-Operator in PigLatin-Programm.
 */
public class PigIndexScanOperator implements IPigOperator {

	/** Zwischenergebnisse. */
	ArrayList<BagInformation> intermediateJoins = null;

	/** Alle Tripel-Muster. */
	Collection<TriplePattern> triplePatternCollection = null;

	/** Counter. */
	int tripleCounter = 0;

	/** Debug Ausgabe. */
	boolean debug = false;

	/**
	 * Instantiates a new pig index scan operator.
	 * 
	 * @param tp
	 *            the tp
	 */
	public PigIndexScanOperator(Collection<TriplePattern> tp) {
		this.triplePatternCollection = tp;
	}

	/**
	 * Mit dieser Methode wird das PigLatin-Programm langsam aufgebaut indem die
	 * einzelnen Tripel-Muster hinzugefügt werden.
	 * 
	 * @param intermediateBags
	 *            the intermediate bags
	 * @param debug
	 *            the debug
	 * @param filterOps
	 *            the filter ops
	 * @return the string
	 */
	public String buildQuery(ArrayList<BagInformation> intermediateBags,
			boolean debug, ArrayList<PigFilterOperator> filterOps) {
		this.intermediateJoins = intermediateBags;
		this.debug = debug;
		StringBuilder result = new StringBuilder();
		for (TriplePattern triplePattern : this.triplePatternCollection) {
			BagInformation curPattern = getHBaseTable(triplePattern);

			if (debug) {
				result.append("-- TriplePattern: " + triplePattern.toN3String()
						+ "\n");
			}
			/**
			 * Für Triplepattern ?s ?p ?o wird eine beliebige Tabelle komplett
			 * geladen und alle Informationen zuürck gegeben.
			 */
			if (curPattern.allElementsAreVariables()) {
				result.append("PATTERN_"
						+ curPattern.getPatternId()
						+ " = "
						+ "load 'hbase://"
						+ curPattern.getTablename()
						+ "' "
						+ "using lupos.cloud.pig.udfs.HBaseLoadUDF('"
						+ HBaseDistributionStrategy.getTableInstance()
								.getColumnFamilyName() + "', '-loadKey true'");
				if (CloudManagement.bloomfilter_active) {
					result.append(", '', "
							+ " '"
							+ DigestUtils.sha512Hex(
									curPattern.getBagElements().get(1)
											+ curPattern.getPatternId())
									.toString()
							+ "', '"
							+ DigestUtils.sha512Hex(
									curPattern.getBagElements().get(2)
											+ curPattern.getPatternId())
									.toString() + "'");
				}

				result.append(") as (rowkey:chararray, columncontent_"
						+ tripleCounter + ":map[]");
				if (CloudManagement.bloomfilter_active) {
					result.append(", bloomfilter1:bytearray, bloomfilter2:bytearray");
				}
				result.append(");\n");
				result.append(curPattern.getName()
						+ " = foreach "
						+ "PATTERN_"
						+ curPattern.getPatternId()
						+ " generate $0, flatten(lupos.cloud.pig.udfs.MapToBagUDF($1");
				if (CloudManagement.bloomfilter_active) {
					result.append(", $2, $3");
				}
				result.append("));\n");
			} else if (curPattern.allElementsAreLiterals()) {
				// do nothing, todo
				return "";
			} else {
				result.append(
				/**
				 * Für alle anderen Triplepattern wird in den jeweiligen
				 * Tabellen gesucht und nur das Ergebniss (der Spaltenname)
				 * zurückgegeben.
				 * 
				 * Anmerkung bzgl Bitvektoren: Statt den Pfad des jeweiligen
				 * Bitvektor wird ein Platzhalter (SHA 512 HEX Wert) eingesetzt.
				 * Dieser Platzhalter wird später bei der Bitvektorberechnung
				 * durch den tatsächlichen Pfad ersetzt. Der Hash wird
				 * folgendermaßen berechnet:
				 * 
				 * h(x) = 
				 */
				"PATTERN_"
						+ curPattern.getPatternId()
						+ " = "
						+ "load 'hbase://"
						+ curPattern.getTablename()
						+ "' "
						+ "using lupos.cloud.pig.udfs.HBaseLoadUDF('"
						+ HBaseDistributionStrategy.getTableInstance()
								.getColumnFamilyName() + "', '', '"
						+ curPattern.getLiterals() + "'");
				if (CloudManagement.bloomfilter_active) {
					result.append(((curPattern.getBagElements().size() == 1) ? ", '"
							+ DigestUtils.sha512Hex(
									curPattern.getBagElements().get(0)
											+ curPattern.getPatternId())
									.toString() + "'" : ", '"
							+ DigestUtils.sha512Hex(
									curPattern.getBagElements().get(0)
											+ curPattern.getPatternId())
									.toString()
							+ "', '"
							+ DigestUtils.sha512Hex(
									curPattern.getBagElements().get(1)
											+ curPattern.getPatternId())
									.toString() + "'"));
				}

				result.append(") as (columncontent_" + tripleCounter + ":map[]");
				if (CloudManagement.bloomfilter_active) {
					result.append(((curPattern.getBagElements().size() == 1) ? ", bloomfilter1:bytearray"
							: ", bloomfilter1:bytearray, bloomfilter2:bytearray"));
				}
				result.append(");\n");

				result.append(curPattern.getName() + " = foreach PATTERN_"
						+ curPattern.getPatternId()
						+ " generate flatten(lupos.cloud.pig.udfs.MapToBagUDF("
						+ "$0");
				if (CloudManagement.bloomfilter_active) {
					result.append(((curPattern.getBagElements().size() == 1) ? ", $1"
							: ", $1, $2"));
				}

				result.append(")) as "
						+ ((curPattern.getBagElements().size() == 1) ? "(output"
								+ tripleCounter + ":chararray);"
								: "(output1_" + tripleCounter
										+ ":chararray, output2_"
										+ tripleCounter + ":chararray); ")
						+ "\n");
			}
			intermediateJoins.add(curPattern);

			// add bitvector
			if ((curPattern.getBagElements().size() == 1)) {
				curPattern.addBitvector(curPattern.getBagElements().get(0),
						new CloudBitvector(curPattern.getTablename(),
								curPattern.getLiterals(),
								BitvectorManager.bloomfilter1ColumnFamily,
								curPattern.getPatternId()));
			} else if ((curPattern.getBagElements().size() == 2)) {
				curPattern.addBitvector(curPattern.getBagElements().get(0),
						new CloudBitvector(curPattern.getTablename(),
								curPattern.getLiterals(),
								BitvectorManager.bloomfilter1ColumnFamily,
								curPattern.getPatternId()));
				curPattern.addBitvector(curPattern.getBagElements().get(1),
						new CloudBitvector(curPattern.getTablename(),
								curPattern.getLiterals(),
								BitvectorManager.bloomfilter2ColumnFamily,
								curPattern.getPatternId()));
			} else if ((curPattern.getBagElements().size() == 3)) {
				curPattern.addBitvector(
						curPattern.getBagElements().get(0),
						new CloudBitvector(curPattern.getTablename(),
								curPattern.getLiterals(), null, curPattern
										.getPatternId()));
				curPattern.addBitvector(
						curPattern.getBagElements().get(1),
						new CloudBitvector(curPattern.getTablename(),
								curPattern.getLiterals(), null, curPattern
										.getPatternId()));
				curPattern.addBitvector(
						curPattern.getBagElements().get(2),
						new CloudBitvector(curPattern.getTablename(),
								curPattern.getLiterals(), null, curPattern
										.getPatternId()));
			}

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
	private BagInformation getHBaseTable(TriplePattern triplePattern) {
		int subject = triplePattern.getSubject().getClass() == Variable.class ? 1
				: 0;
		int predicate = triplePattern.getPredicate().getClass() == Variable.class ? 10
				: 0;
		int object = triplePattern.getObject().getClass() == Variable.class ? 100
				: 0;

		BagInformation result = null;
		switch (subject + predicate + object) {
		case 110:
			result = new BagInformation(triplePattern, "S_PO",
					"INTERMEDIATE_BAG_");
			break;
		case 101:
			result = new BagInformation(triplePattern, "P_SO",
					"INTERMEDIATE_BAG_");
			break;
		case 11:
			result = new BagInformation(triplePattern, "O_SP",
					"INTERMEDIATE_BAG_");
			break;
		case 100:
			result = new BagInformation(triplePattern, "SP_O",
					"INTERMEDIATE_BAG_");
			break;
		case 10:
			result = new BagInformation(triplePattern, "SO_P",
					"INTERMEDIATE_BAG_");
			break;
		case 1:
			result = new BagInformation(triplePattern, "PO_S",
					"INTERMEDIATE_BAG_");
			break;
		case 111:
			// Wenn alles Variablen sind kann eine beliebige Tabelle verwendet
			// werden, hier wird S_PO genommen
			result = new BagInformation(triplePattern, "S_PO",
					"INTERMEDIATE_BAG_");
			break;
		case 0:
			// Wenn alles Literale sind kann eine beliebige Tabelle verwendet
			// werden, hier wird SO_P genommen
			result = new BagInformation(triplePattern, "SO_P",
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
	 * Multi join over two variables.
	 * 
	 * @return the string
	 */
	public String multiJoinOverTwoVariables() {
		StringBuilder result = new StringBuilder();
		HashSet<String> equalVariables = null;
		HashSet<BagInformation> toJoin = new HashSet<BagInformation>();
		boolean found = false;

		/*
		 * Es wird die Join-Menge gesucht bei dem eine Variable am häufigsten
		 * vorkommt. Für die Join-Mengen wird dann ein PigLatin Join ausgegeben
		 * und die Join-Mengen werden zu einer vereinigt.
		 */
		for (BagInformation curJoin1 : intermediateJoins) {
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
			for (BagInformation curJoin2 : intermediateJoins) {
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
				new ArrayList<BagInformation>(toJoin), new ArrayList<String>(
						equalVariables)));

		for (BagInformation toRemove : toJoin) {
			intermediateJoins.remove(toRemove);
		}
		// this.joinVariables.remove(variableToJoin);
		return result.toString();
	}

	/**
	 * Multi join over one variable.
	 * 
	 * @return the string
	 */
	public String multiJoinOverOneVariable() {
		StringBuilder result = new StringBuilder();
		ArrayList<BagInformation> joinAliases = null;
		ArrayList<ArrayList<BagInformation>> joinCandidates = new ArrayList<ArrayList<BagInformation>>();
		ArrayList<String> joinVariablesCandidates = new ArrayList<String>();

		/*
		 * Es wird die Join-Menge gesucht bei dem eine Variable am häufigsten
		 * vorkommt. Für die Join-Mengen wird dann ein PigLatin Join ausgegeben
		 * und die Join-Mengen werden zu einer vereinigt.
		 */
		for (BagInformation curJoin : intermediateJoins) {
			boolean found = false;
			String joinVariable = "";
			joinAliases = new ArrayList<BagInformation>();
			// JoinInformation curJoin = intermediateJoins.get(0);
			joinAliases.add(curJoin);
			for (int i = 0; i < intermediateJoins.size(); i++) {
				if (intermediateJoins.get(i).equals(curJoin)) {
					continue;
				}
				for (String variable1 : curJoin.getBagElements()) {
					if (found) {
						variable1 = joinVariable;
					}
					for (String variable2 : intermediateJoins.get(i)
							.getBagElements()) {
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

		ArrayList<BagInformation> patternToJoin = joinCandidates.get(0);
		String variableToJoin = joinVariablesCandidates.get(0);
		int i = 0;
		for (ArrayList<BagInformation> curCandidate : joinCandidates) {
			if (curCandidate.size() > patternToJoin.size()) {
				patternToJoin = curCandidate;
				variableToJoin = joinVariablesCandidates.get(i);
			}
			i++;
		}

		result.append(getPigMultiJoin(patternToJoin, variableToJoin));

		for (BagInformation toRemove : patternToJoin) {
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
	public String getPigMultiJoin(ArrayList<BagInformation> joinOverItem,
			String joinElement) {
		StringBuilder result = new StringBuilder();

		for (BagInformation bag : joinOverItem) {
			if (bag.isVariableOptional(joinElement)) {
				throw new RuntimeException(
						"Join over optional variable is not allowed in pig!");
			}
		}

		if (debug) {
			result.append("-- Join over " + joinElement.toString() + "\n");
		}

		BagInformation curJoinInfo = new BagInformation("INTERMEDIATE_BAG_"
				+ BagInformation.idCounter);
		result.append(curJoinInfo.getName() + " = JOIN");
		int i = 0;
		for (BagInformation curPattern : joinOverItem) {
			i++;
			for (String s : curPattern.getBagElements()) {
				curJoinInfo.getBagElements().add(s);
			}
			result.append(" " + curPattern.getName() + " BY $"
					+ curPattern.getItemPos(joinElement));
			if (i < joinOverItem.size()) {
				result.append(",");
			} else {
				if (CloudManagement.PARALLEL_REDUCE_OPERATIONS > 1) {
					result.append(" PARALLEL "
							+ CloudManagement.PARALLEL_REDUCE_OPERATIONS);
				}
				result.append(";\n");
			}

			for (String elem : curPattern.getOptionalJoinElements()) {
				curJoinInfo.addOptionalElements(elem);
			}
			curJoinInfo.addBitVectors(curPattern.getBitVectors());
		}
		curJoinInfo.setPatternId(BagInformation.idCounter);
		curJoinInfo.addAppliedFilters(BagInformation
				.mergeAppliedFilters(joinOverItem));

		result.append(removeDuplicatedAliases(curJoinInfo));
		intermediateJoins.add(curJoinInfo);
		BagInformation.idCounter++;

		return result.toString();
	}

	/**
	 * Gets the pig multi join with2 columns.
	 * 
	 * @param joinOverItem
	 *            the join over item
	 * @param joinElements
	 *            the join elements
	 * @return the pig multi join with2 columns
	 */
	public String getPigMultiJoinWith2Columns(
			ArrayList<BagInformation> joinOverItem,
			ArrayList<String> joinElements) {
		StringBuilder result = new StringBuilder();

		for (String var : joinElements) {
			for (BagInformation bag : joinOverItem) {
				if (bag.isVariableOptional(var)) {
					throw new RuntimeException(
							"Join over optional variable is not allowed in pig!");
				}
			}
		}

		if (debug) {
			result.append("-- Join over " + joinElements.toString() + "\n");
		}

		BagInformation curJoinInfo = new BagInformation("INTERMEDIATE_BAG_"
				+ BagInformation.idCounter);

		result.append(curJoinInfo.getName() + " = JOIN");
		int i = 0;
		for (BagInformation curPattern : joinOverItem) {
			i++;
			for (String s : curPattern.getBagElements()) {
				curJoinInfo.getBagElements().add(s);
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

			curJoinInfo.addBitVectors(curPattern.getBitVectors());

		}
		curJoinInfo.setPatternId(BagInformation.idCounter);
		curJoinInfo.addAppliedFilters(BagInformation
				.mergeAppliedFilters(joinOverItem));

		result.append(removeDuplicatedAliases(curJoinInfo));
		intermediateJoins.add(curJoinInfo);
		BagInformation.idCounter++;

		return result.toString();
	}

	/**
	 * Gets the final alias.
	 * 
	 * @return the final alias
	 */
	public String getFinalAlias() {
		return intermediateJoins.get(0).getName();
	}

	// hat keinen Vorteil gebracht
	/**
	 * Removes the duplicated aliases.
	 * 
	 * @param oldJoin
	 *            the old join
	 * @return the string
	 */
	@Deprecated
	public String removeDuplicatedAliases(BagInformation oldJoin) {
		return "";
		// StringBuilder result = new StringBuilder();
		// // prüfe ob es doppelte Aliases gibt und entferne diese
		// ArrayList<String> newElements = new ArrayList<String>();
		// boolean foundDuplicate = false;
		//
		// for (String elem : oldJoin.getJoinElements()) {
		// if (newElements.contains(elem)
		// // Sonderfall z.B. ?author und ?author2 überpruefen
		// && elem.equals(newElements.get(newElements.indexOf(elem)))) {
		// foundDuplicate = true;
		// } else {
		// newElements.add(elem);
		// }
		// }
		//
		// System.out.println("V: " + oldJoin.getJoinElements());
		// System.out.println("N: " + newElements);
		// if (foundDuplicate) {
		// result.append(oldJoin.getName() + " = FOREACH " + oldJoin.getName()
		// + " GENERATE ");
		// boolean first = true;
		// for (String elem : newElements) {
		// if (!first) {
		// result.append(", ");
		// }
		// result.append("$" + oldJoin.getItemPos(elem));
		// first = false;
		// }
		// result.append(";\n");
		// oldJoin.setJoinElements(newElements);
		// }
		//
		// return result.toString();

	}
}
