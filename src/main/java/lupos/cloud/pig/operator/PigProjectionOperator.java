package lupos.cloud.pig.operator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import lupos.cloud.pig.JoinInformation;
import lupos.cloud.pig.SinglePigQuery;
import lupos.datastructures.items.Variable;
import lupos.engine.operators.singleinput.Projection;

public class PigProjectionOperator implements IPigOperator {
	HashSet<String> projectionVariables;
	private ArrayList<JoinInformation> intermediateJoins;
	private boolean debug;
	ArrayList<PigFilterOperator> filterOps;

	public PigProjectionOperator(HashSet<Variable> projection) {
		this.projectionVariables = new HashSet<String>();
		for (Variable varToAdd : projection) {
			this.projectionVariables.add(varToAdd.toString());
		}
	}

	public void addProjectionVaribles(HashSet<String> variables) {
		for (String varToAdd : variables) {
			this.projectionVariables.add(varToAdd);
		}
	}

	public HashSet<String> getProjectionVariables() {
		return projectionVariables;
	}

	@Override
	public String buildQuery(ArrayList<JoinInformation> intermediateBags,
			boolean debug, ArrayList<PigFilterOperator> filterOps) {
		this.intermediateJoins = intermediateBags;
		this.filterOps = filterOps;
		this.debug = debug;
		return this.checkIfProjectionPossible();
	}

	private String checkIfProjectionPossible() {
		StringBuilder result = new StringBuilder();
		if (projectionVariables.size() != 0) {
			HashMap<JoinInformation, HashSet<String>> varJoinMap = getValidProjectionVariables();

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

					JoinInformation newJoin = new JoinInformation(
							"INTERMEDIATE_BAG_" + JoinInformation.idCounter);

					result.append(newJoin.getName() + " = FOREACH "
							+ curJoin.getName() + " GENERATE ");

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
					newJoin.setJoinElements(new ArrayList<String>(varJoinMap
							.get(curJoin)));

					newJoin.mergeOptionalVariables(curJoin);

					intermediateJoins.remove(curJoin);
					intermediateJoins.add(newJoin);
					JoinInformation.idCounter++;
				}
			}
		}
		return result.toString();
	}

	private HashMap<JoinInformation, HashSet<String>> getValidProjectionVariables() {
		HashMap<JoinInformation, HashSet<String>> varJoinMap = new HashMap<JoinInformation, HashSet<String>>();
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
				HashSet<String> dropNotAllowedList = new HashSet<String>();
				for (String dropCandidateVariable : projectionJoin
						.getJoinElements()) {
					// Variable wird noch für Joins mit anderen Bags gebraucht?
					for (JoinInformation otherJoin : intermediateJoins) {
						if (!otherJoin.equals(projectionJoin)) {
							if (otherJoin.getJoinElements().contains(
									dropCandidateVariable)) {
								dropNotAllowedList.add(dropCandidateVariable);
							}

						}
					}

					// Variable wird noch für einen Filter gebraucht?
					for (PigFilterOperator pigFilter : this.filterOps) {
						for (String filterVar : pigFilter.getVariables()) {
							if (dropCandidateVariable.equals(filterVar)) {
								// Wenn eine Filtervariable gedropt werden soll
								// überprüfe ob Filter schon angewendet wurde,
								// wenn ja kann sie gedroppt weden
								for (JoinInformation join : intermediateJoins) {
									if (join.getJoinElements().contains(
											filterVar)
											&& !join.getAppliedFilters()
													.contains(pigFilter)) {
										dropNotAllowedList
												.add(dropCandidateVariable);
									}

								}

							}
						}
					}

				}

				// Wende die Projetion nur an, wenn sich die Liste
				// verkleinert
				if (dropNotAllowedList.size() + 1 < projectionJoin
						.getJoinElements().size()) {

					HashSet<String> varList = varJoinMap.get(projectionJoin);
					if (varList != null) {
						varList.add(projectionVar);
					} else {
						HashSet<String> newList = new HashSet<String>();
						newList.add(projectionVar);
						varJoinMap.put(projectionJoin, newList);
					}

					for (String noDrop : dropNotAllowedList) {
						varJoinMap.get(projectionJoin).add(noDrop);
					}
				}
			}
		}
		return varJoinMap;
	}

	private boolean joinListAndProjectionListAreEquals(
			ArrayList<String> joinElements, HashSet<String> compareList) {
		if (joinElements.size() != compareList.size()) {
			return false;
		}
		for (String elem : joinElements) {
			if (!compareList.contains(elem)) {
				return false;
			}
		}
		return true;
	}

	public void replaceVariableInProjection(HashMap<String, String> addBinding) {
		if (addBinding != null) {
			for (String oldVar : addBinding.keySet()) {
				boolean removed = this.projectionVariables.remove(oldVar);
				if (removed) {
					this.projectionVariables.add(addBinding.get(oldVar));
				}
			}
		}
	}
}
