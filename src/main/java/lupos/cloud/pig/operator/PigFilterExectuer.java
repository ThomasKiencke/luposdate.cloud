package lupos.cloud.pig.operator;

import java.util.ArrayList;
import java.util.HashMap;

import lupos.cloud.pig.BagInformation;
import lupos.cloud.pig.SinglePigQuery;

public class PigFilterExectuer implements IPigOperator {
	private boolean debug;
	private ArrayList<BagInformation> intermediateBags;
	HashMap<BagInformation, ArrayList<PigFilterOperator>> bagToFilterList = null;

	public String buildQuery(ArrayList<BagInformation> intermediateBags,
			boolean debug, ArrayList<PigFilterOperator> filterOps) {
		StringBuilder result = new StringBuilder();
		this.bagToFilterList = new HashMap<BagInformation, ArrayList<PigFilterOperator>>();
		this.debug = debug;
		this.intermediateBags = intermediateBags;
		ArrayList<BagInformation> toRemove = new ArrayList<BagInformation>();
		ArrayList<BagInformation> toAdd = new ArrayList<BagInformation>();

		for (PigFilterOperator filter : filterOps) {
			this.checkIfFilterPossible(filter);
		}
		if (bagToFilterList.size() > 0) {
			for (BagInformation curJoin : bagToFilterList.keySet()) {
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

				BagInformation newJoin = new BagInformation(
						"INTERMEDIATE_BAG_" + BagInformation.idCounter);

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

				newJoin.setPatternId(BagInformation.idCounter);
				newJoin.setJoinElements(curJoin.getJoinElements());
				newJoin.addAppliedFilters(curJoin.getAppliedFilters());
				newJoin.addBitVectors(curJoin.getBitVectors());

				toRemove.add(curJoin);

//				toRemove.remove(curJoin);
				toAdd.add(newJoin);

				BagInformation.idCounter++;

				for (BagInformation item : toRemove) {
					intermediateBags.remove(item);
				}

				for (BagInformation item : toAdd) {
					intermediateBags.add(item);
				}
			}
		}
		return result.toString();
	}

	private void checkIfFilterPossible(PigFilterOperator filter) {
		// Wenn alle Variablen in einer Menge vorkommen, kann der Filter
		// angewandt weden
		for (BagInformation curJoin : intermediateBags) {

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

	private void addFilterToBag(BagInformation toAdd, PigFilterOperator filter) {
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
