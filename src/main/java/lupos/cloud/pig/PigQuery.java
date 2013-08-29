package lupos.cloud.pig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import lupos.cloud.operator.MultiIndexScanContainer;
import lupos.cloud.pig.operator.PigDistinctOperator;
import lupos.cloud.pig.operator.PigFilterOperator;
import lupos.cloud.pig.operator.PigIndexScanOperator;
import lupos.cloud.pig.operator.IPigOperator;
import lupos.cloud.pig.operator.PigLimitOperator;
import lupos.cloud.pig.operator.PigProjectionOperator;
import lupos.engine.operators.BasicOperator;
import lupos.engine.operators.index.BasicIndexScan;

/**
 * In dieser Klassen werden Informationen über das PigQuery abgespeichert z.B.
 * das PigLatin Programm selbst sowie die zu erwartende Variablenliste.
 */
public class PigQuery {

	/** The pig latin. */
	StringBuilder pigLatin = new StringBuilder();

	private PigProjectionOperator projection = null;

	private ArrayList<PigFilterOperator> filterPigOps = new ArrayList<PigFilterOperator>();

	private HashMap<Integer, PigIndexScanOperator> indexScanOps = new HashMap<Integer, PigIndexScanOperator>();

	/** The intermediate joins. */
	private ArrayList<JoinInformation> intermediateBags = new ArrayList<JoinInformation>();

	private MultiIndexScanContainer multiIndexScanContainer = null;

	public boolean debug = true;

	private PigDistinctOperator distinctOperator = null;

	private PigLimitOperator limitOperator;

	private PigIndexScanOperator activeIndexScan = null;

	/**
	 * Gets the pig latin.
	 * 
	 * @return the pig latin
	 */
	public String getPigLatin() {
		return pigLatin.toString();
	}

	public void applyJoins() {
		if (indexScanOps.size() == 1 && multiIndexScanContainer == null) {
			this.multiJoin(indexScanOps.get(0));
		} else {
			this.joinMultiIndexScans(multiIndexScanContainer);
		}

	}

	public JoinInformation joinMultiIndexScans(MultiIndexScanContainer container) {
		JoinInformation newJoin = null;
		for (Integer id : container.getOperatorList().keySet()) {
			HashSet<BasicOperator> curList = container.getOperatorList()
					.get(id);
			ArrayList<JoinInformation> multiInputist = new ArrayList<JoinInformation>();
			for (BasicOperator op : curList) {
				if (op instanceof BasicIndexScan) {
					PigIndexScanOperator pigIndexScan = new PigIndexScanOperator(
							((BasicIndexScan) op).getTriplePattern());
					this.buildAndAppendQuery(pigIndexScan);
					this.addIndexScanOperator(pigIndexScan);
					this.multiJoin(pigIndexScan);
					multiInputist.add(pigIndexScan.getIntermediateJoins()
							.get(0));
					// return pigIndexScan.getIntermediateJoins().get(0);
				} else if (op instanceof MultiIndexScanContainer) {
					final MultiIndexScanContainer c = (MultiIndexScanContainer) op;
					multiInputist.add(this.joinMultiIndexScans(c));
				}
			}

			newJoin = new JoinInformation();
			pigLatin.append(newJoin.getName() + " = ");
			if (container.getMappingTree().get(id)
					.equals(MultiIndexScanContainer.UNION)) {
				if (debug) {
					pigLatin.append(" -- UNION:\n");
				}
				pigLatin.append("UNION ");
				for (int i = 0; i < multiInputist.size(); i++) {
					if (i == 0) {
						pigLatin.append(multiInputist.get(i).getName());
					} else {
						pigLatin.append(", " + multiInputist.get(i).getName());
					}
				}
				pigLatin.append(";\n");
				newJoin.setJoinElements(multiInputist.get(0).getJoinElements());
			} else if (container.getMappingTree().get(id)
					.equals(MultiIndexScanContainer.JOIN)) {
//				if (debug) {
//					pigLatin.append(" -- JOIN:\n");
//				}
//				pigLatin.append("JOIN ");
//				for (int i = 0; i < multiInputist.size(); i++) {
//					if (i == 0) {
//						pigLatin.append(multiInputist.get(i).getName());
//					} else {
//						pigLatin.append(", " + multiInputist.get(i).getName());
//					}
//				}
//				pigLatin.append(";\n");
//				newJoin.setJoinElements(multiInputist.get(0).getJoinElements());

			} else {
//				throw new RuntimeException(
//						"Something is wrong here. Forgot case?");
			}
			pigLatin.append("\n");
			this.intermediateBags.add(newJoin);

			for (JoinInformation toRemove : multiInputist) {
				this.intermediateBags.remove(toRemove);
			}
			
			for (PigFilterOperator filter : filterPigOps) {
				this.buildAndAppendQuery(filter);
			}
			if (projection != null) {
				this.buildAndAppendQuery(projection);
			}

		}
		return newJoin; // never reached
	}

	public void addMultiIndexScanList(MultiIndexScanContainer container) {
		// this.multiIndexScanList = multiIndexScanList;
		// this.mappingTree = mappingTree;
		this.multiIndexScanContainer = container;
	}

	public void addFilter(PigFilterOperator pigFilter) {
		this.filterPigOps.add(pigFilter);
	}

	public void setProjection(PigProjectionOperator projection) {
		this.projection = projection;
	}

	public void finishQuery() {

		// Filter
		for (PigFilterOperator filter : filterPigOps) {
			this.buildAndAppendQuery(filter);
		}

		// Projektion
		if (projection != null) {
			this.buildAndAppendQuery(projection);
		}

		if (distinctOperator != null) {
			this.buildAndAppendQuery(distinctOperator);
		}

		if (limitOperator != null) {
			this.buildAndAppendQuery(limitOperator);
		}

		StringBuilder modifiedPigQuery = new StringBuilder();
		modifiedPigQuery.append(this.pigLatin.toString().replace(
				this.getFinalAlias(), "X"));
		this.pigLatin = modifiedPigQuery;

	}

	public ArrayList<JoinInformation> getIntermediateJoins() {
		if (activeIndexScan == null) {
			return this.intermediateBags;
		} else {
			return activeIndexScan.getIntermediateJoins();
		}
	}

	public boolean isDebug() {
		return debug;
	}

	/**
	 * Multi join über alle Tripel-Muster. Dabei wird zuerst über die Variable
	 * gejoint die in den meisten Tripel-Pattern vorkommt usw.
	 * 
	 * @return the string
	 */
	private void multiJoin(PigIndexScanOperator pigIndexScanOp) {
		this.activeIndexScan = pigIndexScanOp;
		// suche so lange bis es noch Mengen zum joinen gibt
		while (pigIndexScanOp.getIntermediateJoins().size() > 1) {
			/*
			 * Überprüfe bei jeden durchlauf ob eine Projektion/Filter
			 * durchgeführt werden kann (Grund: Projektion so früh wie möglich)
			 */

			for (PigFilterOperator filter : filterPigOps) {
				this.buildAndAppendQuery(filter);
			}
			if (projection != null) {
				this.buildAndAppendQuery(projection);
			}

			// System.out.println("size: " + intermediateJoins.size());
			String multiJoinOverTwoVars = pigIndexScanOp
					.multiJoinOverTwoVariablse();

			/*
			 * Es werden immer erst Tripel-Muster gesucht bei denen über zwei
			 * Variablen gejoint werden kann und erst dann die Muster wo über
			 * eine Variable gejoint wird. Beispiel: {?s ?p ?o . <literal> ?p
			 * ?o}
			 */

			if (debug) {
				this.pigLatin.append("-- Join \n");
			}
			if (multiJoinOverTwoVars != null) {
				this.pigLatin.append(multiJoinOverTwoVars);
			} else {
				pigLatin.append(pigIndexScanOp.multiJoinOverOneVariable());
			}

			if (debug) {
				this.pigLatin.append("\n");
			}

			for (PigFilterOperator filter : filterPigOps) {
				this.buildAndAppendQuery(filter);
			}
			if (projection != null) {
				this.buildAndAppendQuery(projection);
			}
		}

		this.intermediateBags.add(pigIndexScanOp.getIntermediateJoins().get(0));
		this.activeIndexScan = null;
	}

	/**
	 * Gibt die Variablenreihenfolge zurück.
	 * 
	 * @return the result order
	 */
	public ArrayList<String> getVariableList() {
		ArrayList<String> result = new ArrayList<String>();
		for (String elem : intermediateBags.get(0).getJoinElements()) {
			result.add(elem.replace("?", ""));
		}
		return result;
	}

	public String getFinalAlias() {
		return intermediateBags.get(0).getName();
	}

	public void buildAndAppendQuery(IPigOperator operator) {
		this.pigLatin.append(operator.buildQuery(this));
	}

	public void addIndexScanOperator(PigIndexScanOperator pigIndexScan) {
		this.indexScanOps.put(pigIndexScan.getId(), pigIndexScan);
	}

	// public void addIndexScanOperator(String type, PigIndexScanOperator[]
	// pigIndexScans) {
	// for (PigIndexScanOperator)
	// this.indexScanOps.add(pigIndexScan);
	// }

	public void setDistinctOperator(PigDistinctOperator pigDistinctOperator) {
		this.distinctOperator = pigDistinctOperator;
	}

	public void setLimitOperator(PigLimitOperator pigLimitOperator) {
		this.limitOperator = pigLimitOperator;
	}

	public ArrayList<PigFilterOperator> getFilterPigOps() {
		return filterPigOps;
	}

	public PigProjectionOperator getProjection() {
		return projection;
	}
}
