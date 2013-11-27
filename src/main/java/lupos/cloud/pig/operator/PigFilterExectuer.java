package lupos.cloud.pig.operator;

import java.util.ArrayList;
import java.util.HashMap;

import lupos.cloud.pig.BagInformation;

/**
 * Diese Klasse verweist auf alle vorhandenen Filterausdrücke. Die Filter werden
 * möglichst früh ausgeführt. Dazu wird für jeden Filter überprüft ob alle
 * relevanten Variablen sich in der jeweiligen Bag befinden um den Filter
 * auszuführen.
 */
public class PigFilterExectuer implements IPigOperator {

	/** Zwischenergebnisse. */
	private ArrayList<BagInformation> intermediateBags;

	/** Liste der Filter-Operationen. */
	HashMap<BagInformation, ArrayList<PigFilterOperator>> bagToFilterList = null;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * lupos.cloud.pig.operator.IPigOperator#buildQuery(java.util.ArrayList,
	 * boolean, java.util.ArrayList)
	 */
	public String buildQuery(ArrayList<BagInformation> intermediateBags,
			boolean debug, ArrayList<PigFilterOperator> filterOps) {
		StringBuilder result = new StringBuilder();
		this.bagToFilterList = new HashMap<BagInformation, ArrayList<PigFilterOperator>>();
		this.intermediateBags = intermediateBags;
		ArrayList<BagInformation> toRemove = new ArrayList<BagInformation>();
		ArrayList<BagInformation> toAdd = new ArrayList<BagInformation>();

		for (PigFilterOperator filter : filterOps) {
			this.checkIfFilterPossible(filter);
		}
		if (bagToFilterList.size() > 0) {
			for (BagInformation curBag : bagToFilterList.keySet()) {
				if (debug) {
					int i = 0;
					result.append("-- Filter: ");
					for (PigFilterOperator filter : bagToFilterList
							.get(curBag)) {
						if (i > 0) {
							result.append(" AND ");
						}
						result.append(filter.getPigFilter());
						i++;
					}
					result.append("\n");
				}

				BagInformation newBag = new BagInformation("INTERMEDIATE_BAG_"
						+ BagInformation.idCounter);

				result.append(newBag.getName() + " = FILTER "
						+ curBag.getName() + " BY ");
				int i = 0;
				for (PigFilterOperator filter : bagToFilterList.get(curBag)) {
					if (i > 0) {
						result.append(" AND ");
					}
					String pigFilterVarReplaced = filter.getPigFilter();
					for (String var : filter.getVariables()) {
						pigFilterVarReplaced = pigFilterVarReplaced.replace(
								var,
								getPigNameForVariable("?" + var,
										curBag.getBagElements()));
					}

					result.append(pigFilterVarReplaced);

					newBag.addAppliedFilters(filter);
					curBag.addAppliedFilters(filter);

					i++;
				}

				result.append(";\n");

				if (debug) {
					result.append("\n");
				}

				newBag.setPatternId(BagInformation.idCounter);
				newBag.setJoinElements(curBag.getBagElements());
				newBag.addAppliedFilters(curBag.getAppliedFilters());
				newBag.addBitVectors(curBag.getBitVectors());

				toRemove.add(curBag);

				// toRemove.remove(curBag);
				toAdd.add(newBag);

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

	/**
	 * Check if filter possible.
	 * 
	 * @param filter
	 *            the filter
	 */
	private void checkIfFilterPossible(PigFilterOperator filter) {
		// Wenn alle Variablen in einer Menge vorkommen, kann der Filter
		// angewandt weden
		for (BagInformation curBag : intermediateBags) {

			// Wenn die Menge nicht schon einmal gefiltert wurde
			if (!curBag.filterApplied(filter)) {
				boolean varNotFound = false;
				for (String var : filter.getVariables()) {
					if (!curBag.getBagElements().contains("?" + var)) {
						varNotFound = true;
					}
				}
				if (!varNotFound) {
					addFilterToBag(curBag, filter);

				}
			}
		}

	}

	/**
	 * Gets the pig index for variable.
	 * 
	 * @param name
	 *            the name
	 * @param sparqlVariableList
	 *            the sparql variable list
	 * @return the pig name for variable
	 */
	private String getPigNameForVariable(String name,
			ArrayList<String> sparqlVariableList) {
		for (int i = 0; i < sparqlVariableList.size(); i++) {
			if (sparqlVariableList.get(i).equals(name)) {
				return "$" + i;
			}
		}
		return null; // Fall sollte nicht vorkommen
	}

	/**
	 * Adds the filter to bag.
	 * 
	 * @param toAdd
	 *            the to add
	 * @param filter
	 *            the filter
	 */
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
