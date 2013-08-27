package lupos.cloud.pig;

import java.util.ArrayList;

import lupos.cloud.pig.operator.PigDistinctOperator;
import lupos.cloud.pig.operator.PigFilterOperator;
import lupos.cloud.pig.operator.PigIndexScanOperator;
import lupos.cloud.pig.operator.IPigOperator;
import lupos.cloud.pig.operator.PigLimitOperator;
import lupos.cloud.pig.operator.PigProjectionOperator;

/**
 * In dieser Klassen werden Informationen über das PigQuery abgespeichert z.B.
 * das PigLatin Programm selbst sowie die zu erwartende Variablenliste.
 */
public class PigQuery {

	/** The pig latin. */
	StringBuilder pigLatin = new StringBuilder();

	private PigProjectionOperator projection = null;

	private ArrayList<PigFilterOperator> filterPigOps = new ArrayList<PigFilterOperator>();

	private PigIndexScanOperator indexScanOperator = null;

	/** The intermediate joins. */
	ArrayList<JoinInformation> intermediateJoins = new ArrayList<JoinInformation>();

	public boolean debug = true;

	private PigDistinctOperator distinctOperator = null;

	private PigLimitOperator limitOperator;

	/**
	 * Gets the pig latin.
	 * 
	 * @return the pig latin
	 */
	public String getPigLatin() {
		return pigLatin.toString();
	}

	public void applyJoins() {
		this.multiJoin();
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
		return intermediateJoins;
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
	private void multiJoin() {
		// suche so lange bis es noch Mengen zum joinen gibt
		while (intermediateJoins.size() > 1) {
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
			String multiJoinOverTwoVars = indexScanOperator
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
				pigLatin.append(indexScanOperator.multiJoinOverOneVariable());
			}

			if (debug) {
				this.pigLatin.append("\n");
			}
		}
	}

	/**
	 * Gibt die Variablenreihenfolge zurück.
	 * 
	 * @return the result order
	 */
	public ArrayList<String> getVariableList() {
		ArrayList<String> result = new ArrayList<String>();
		for (String elem : intermediateJoins.get(0).getJoinElements()) {
			result.add(elem.replace("?", ""));
		}
		return result;
	}

	public String getFinalAlias() {
		return intermediateJoins.get(0).getName();
	}

	public void buildAndAppendQuery(IPigOperator operator) {
		this.pigLatin.append(operator.buildQuery(this));
	}

	public void setIndexScanOperator(PigIndexScanOperator pigIndexScan) {
		this.indexScanOperator = pigIndexScan;
	}

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
