package lupos.cloud.pig.operator;

import java.util.ArrayList;
import java.util.HashMap;

import lupos.cloud.pig.JoinInformation;
import lupos.cloud.pig.SinglePigQuery;

public class PigFilterExectuer implements IPigOperator {
	private boolean debug;
	private ArrayList<JoinInformation> intermediateBags;
	HashMap<JoinInformation, ArrayList<PigFilterOperator>> bagToFilterList = null;

	public String buildQuery(ArrayList<JoinInformation> intermediateBags,
			boolean debug, ArrayList<PigFilterOperator> filterOps) {
		StringBuilder result = new StringBuilder();
		this.bagToFilterList = new HashMap<JoinInformation, ArrayList<PigFilterOperator>>();
		this.debug = debug;
		this.intermediateBags = intermediateBags;
		ArrayList<JoinInformation> toRemove = new ArrayList<JoinInformation>();
		ArrayList<JoinInformation> toAdd = new ArrayList<JoinInformation>();

		for (PigFilterOperator filter : filterOps) {
			this.checkIfFilterPossible(filter);
		}
		if (bagToFilterList.size() > 0) {
			for (JoinInformation curJoin : bagToFilterList.keySet()) {
				if (debug) {
					int i = 0;
					result.append("-- Filter: ");
					for (PigFilterOperator filter : bagToFilterList
							.get(curJoin)) {
						if (i > 0) {
							result.append(" AND ");
						}
						result.append(filter.getPigFilter());
						i++;
					}
					result.append("\n");
				}

				JoinInformation newJoin = new JoinInformation(
						"INTERMEDIATE_BAG_" + JoinInformation.idCounter);

				result.append(newJoin.getName() + " = FILTER "
						+ curJoin.getName() + " BY ");
				int i = 0;
				for (PigFilterOperator filter : bagToFilterList.get(curJoin)) {
					if (i > 0) {
						result.append(" AND ");
					}
					String pigFilterVarReplaced = filter.getPigFilter();
					for (String var : filter.getVariables()) {
						pigFilterVarReplaced = pigFilterVarReplaced
								.replace(
										var,
										getPigNameForVariable("?" + var,
												curJoin.getJoinElements()));
					}
					
					result.append(pigFilterVarReplaced);

					newJoin.addAppliedFilters(filter);
					curJoin.addAppliedFilters(filter);
					
					i++;
				}

				result.append(";\n");

				if (debug) {
					result.append("\n");
				}

				newJoin.setPatternId(JoinInformation.idCounter);
				newJoin.setJoinElements(curJoin.getJoinElements());
				newJoin.addAppliedFilters(curJoin.getAppliedFilters());
				newJoin.setBitVectors(curJoin.getBitVectors());

				toRemove.add(curJoin);

//				toRemove.remove(curJoin);
				toAdd.add(newJoin);

				JoinInformation.idCounter++;

				for (JoinInformation item : toRemove) {
					intermediateBags.remove(item);
				}

				for (JoinInformation item : toAdd) {
					intermediateBags.add(item);
				}
			}
		}
		return result.toString();
	}

	private void checkIfFilterPossible(PigFilterOperator filter) {
		// Wenn alle Variablen in einer Menge vorkommen, kann der Filter
		// angewandt weden
		for (JoinInformation curJoin : intermediateBags) {

			// Wenn die Menge nicht schon einmal gefiltert wurde
			if (!curJoin.filterApplied(filter)) {
				boolean varNotFound = false;
				for (String var : filter.getVariables()) {
					if (!curJoin.getJoinElements().contains("?" + var)) {
						varNotFound = true;
					}
				}
				if (!varNotFound) {
					addFilterToBag(curJoin, filter);

				}
			}
		}

	}

	private String getPigNameForVariable(String name,
			ArrayList<String> sparqlVariableList) {
		for (int i = 0; i < sparqlVariableList.size(); i++) {
			if (sparqlVariableList.get(i).equals(name)) {
				return "$" + i;
			}
		}
		return null; // Fall sollte nicht vorkommen
	}

	private void addFilterToBag(JoinInformation toAdd, PigFilterOperator filter) {
		ArrayList<PigFilterOperator> list = bagToFilterList.get(toAdd);
		if (list == null) {
			list = new ArrayList<PigFilterOperator>();
			list.add(filter);
			this.bagToFilterList.put(toAdd, list);
		} else {
			list.add(filter);
		}

	}

}
