package lupos.cloud.pig.operator;

import java.util.ArrayList;
import java.util.HashMap;

import lupos.cloud.pig.JoinInformation;
import lupos.cloud.pig.PigQuery;
import lupos.datastructures.items.Variable;
import lupos.engine.operators.singleinput.Projection;

public class PigProjectionOperator implements IPigOperator {
	ArrayList<String> projectionVariables = new ArrayList<String>();
	private ArrayList<JoinInformation> intermediateJoins;
	private boolean debug;

	public PigProjectionOperator(Projection projection) {
		this.projectionVariables = new ArrayList<String>();
		for (Variable varToAdd : projection.getProjectedVariables()) {
			this.projectionVariables.add(varToAdd.toString());
		}
	}

	@Override
	public String buildQuery(PigQuery pigQuery) {
		this.intermediateJoins = pigQuery.getIntermediateJoins();
		this.debug = pigQuery.isDebug();
		return this.checkIfProjectionPossible();
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
								dropNoteAllowedList.add(dropCandidateVariable);
							}

						}
					}
				}

				// Wende die Projetion nur an, wenn sich die Liste
				// verkleinert
				if (dropNoteAllowedList.size() + 1 < projectionJoin
						.getJoinElements().size()) {

					ArrayList<String> varList = varJoinMap.get(projectionJoin);
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
}
