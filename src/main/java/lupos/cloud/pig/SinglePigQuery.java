package lupos.cloud.pig;

import java.util.ArrayList;
import java.util.HashMap;

import lupos.cloud.pig.operator.PigDistinctOperator;
import lupos.cloud.pig.operator.PigFilterExectuer;
import lupos.cloud.pig.operator.PigFilterOperator;
import lupos.cloud.pig.operator.PigIndexScanOperator;
import lupos.cloud.pig.operator.IPigOperator;
import lupos.cloud.pig.operator.PigLimitOperator;
import lupos.cloud.pig.operator.PigOrderByOperator;
import lupos.cloud.pig.operator.PigProjectionOperator;

/**
 * Für jedes BGP wird ein SinglePigQuery erzeugt.
 * 
 */
public class SinglePigQuery {

	/** PigLatin-Programm. */
	StringBuilder pigLatin = new StringBuilder();

	/** Projektion die für den Container gültig ist */
	private PigProjectionOperator globalProjection = null;

	/** Filter die für den gesamten SubGraph Container gültig sind */
	private ArrayList<PigFilterOperator> globalFilterPigOp = new ArrayList<PigFilterOperator>();

	/** IndexScan-Operation des aktuellen BGP. */
	PigIndexScanOperator indexScanOp = null;

	/** Zwischenergebnisse */
	private ArrayList<BagInformation> intermediateBags = new ArrayList<BagInformation>();

	/**  debug. */
	public boolean debug = true;

	/** Distinct Operator. */
	private PigDistinctOperator distinctOperator = null;

	/** Limit Operator. */
	private PigLimitOperator limitOperator;

	/** Sort Operator. */
	private PigOrderByOperator pigOrderByOperator;

	/** liste der Bindings. */
	private HashMap<String, String> addBinding = null;

	/**
	 * Gets the pig latin.
	 * 
	 * @return the pig latin
	 */
	public String getPigLatin() {
		return pigLatin.toString();
	}

	/**
	 * Apply joins.
	 */
	public void applyJoins() {
		this.multiJoin();

	}

	/**
	 * Finish query.
	 */
	public void finishQuery() {

		this.applyJoins();

		if (distinctOperator != null) {
			this.buildAndAppendQuery(distinctOperator);
		}

		if (pigOrderByOperator != null) {
			this.buildAndAppendQuery(pigOrderByOperator);
		}

		if (limitOperator != null) {
			this.buildAndAppendQuery(limitOperator);
		}

		executeFiltersAndProjections(globalProjection, globalFilterPigOp);
	}

	/**
	 * Gets the intermediate joins.
	 * 
	 * @return the intermediate joins
	 */
	public ArrayList<BagInformation> getIntermediateJoins() {
		return this.intermediateBags;
	}

	/**
	 * Checks if is debug.
	 * 
	 * @return true, if is debug
	 */
	public boolean isDebug() {
		return debug;
	}

	/**
	 * Execute filters and projections.
	 * 
	 * @param projection
	 *            the projection
	 * @param filterOps
	 *            the filter ops
	 */
	private void executeFiltersAndProjections(PigProjectionOperator projection,
			ArrayList<PigFilterOperator> filterOps) {
		// Filter
		this.buildAndAppendQuery(new PigFilterExectuer());

		// Projection
		if (projection != null) {
			this.buildAndAppendQuery(projection);
		}
	}

	/**
	 * Multi join über alle Tripel-Muster. Dabei wird zuerst über die Variable
	 * gejoint die in den meisten Tripel-Pattern vorkommt usw.
	 * 
	 * @return the string
	 */
	private void multiJoin() {
		// suche so lange bis es noch Mengen zum joinen gibt
		while (intermediateBags.size() > 1) {
			/*
			 * Überprüfe bei jeden durchlauf ob eine Projektion/Filter
			 * durchgeführt werden kann (Grund: Projektion so früh wie möglich)
			 */

			// push filter/projection
			executeFiltersAndProjections(globalProjection, globalFilterPigOp);

			// System.out.println("size: " + intermediateJoins.size());
			String multiJoinOverTwoVars = this.indexScanOp
					.multiJoinOverTwoVariables();

			/*
			 * Es werden immer erst Tripel-Muster gesucht bei denen über zwei
			 * Variablen gejoint werden kann und erst dann die Muster wo über
			 * eine Variable gejoint wird. Beispiel: {?s ?p ?o . <literal> ?p
			 * ?o}
			 */

			if (multiJoinOverTwoVars != null) {
				this.pigLatin.append(multiJoinOverTwoVars);
			} else {
				pigLatin.append(this.indexScanOp.multiJoinOverOneVariable());
			}

			if (debug) {
				this.pigLatin.append("\n");
			}
		}

		executeFiltersAndProjections(globalProjection, globalFilterPigOp);
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

	/**
	 * Builds the and append query.
	 * 
	 * @param operator
	 *            the operator
	 */
	public void buildAndAppendQuery(IPigOperator operator) {
		this.pigLatin.append(operator.buildQuery(this.intermediateBags,
				this.debug, this.globalFilterPigOp));
	}

	/**
	 * Sets the index scan operator.
	 * 
	 * @param pigIndexScan
	 *            the new index scan operator
	 */
	public void setIndexScanOperator(PigIndexScanOperator pigIndexScan) {
		this.indexScanOp = pigIndexScan;
	}

	/**
	 * Sets the distinct operator.
	 * 
	 * @param pigDistinctOperator
	 *            the new distinct operator
	 */
	public void setDistinctOperator(PigDistinctOperator pigDistinctOperator) {
		this.distinctOperator = pigDistinctOperator;
	}

	/**
	 * Sets the limit operator.
	 * 
	 * @param pigLimitOperator
	 *            the new limit operator
	 */
	public void setLimitOperator(PigLimitOperator pigLimitOperator) {
		this.limitOperator = pigLimitOperator;
	}

	/**
	 * Gets the filter pig ops.
	 * 
	 * @return the filter pig ops
	 */
	public ArrayList<PigFilterOperator> getFilterPigOps() {
		return globalFilterPigOp;
	}

	/**
	 * Gets the projection.
	 * 
	 * @return the projection
	 */
	public PigProjectionOperator getProjection() {
		return globalProjection;
	}

	/**
	 * Sets the container projection.
	 * 
	 * @param pigProjectionOperator
	 *            the new container projection
	 */
	public void setContainerProjection(
			PigProjectionOperator pigProjectionOperator) {
		this.globalProjection = pigProjectionOperator;
	}

	/**
	 * Adds the container filter.
	 * 
	 * @param pigFilter
	 *            the pig filter
	 */
	public void addContainerFilter(PigFilterOperator pigFilter) {
		this.globalFilterPigOp.add(pigFilter);
	}

	/**
	 * Adds the projection.
	 * 
	 * @param newProjection
	 *            the new projection
	 */
	public void addProjection(PigProjectionOperator newProjection) {
		if (globalProjection == null) {
			this.globalProjection = newProjection;
		} else {
			this.globalProjection.addProjectionVaribles(newProjection
					.getProjectionVariables());
		}
		this.globalProjection.replaceVariableInProjection(this.addBinding);
	}

	/**
	 * Sets the orderby operator.
	 * 
	 * @param pigOrderByOperator
	 *            the new orderby operator
	 */
	public void setOrderbyOperator(PigOrderByOperator pigOrderByOperator) {
		this.pigOrderByOperator = pigOrderByOperator;
	}

	/**
	 * Replace variable in projection.
	 * 
	 * @param oldVar
	 *            the old var
	 * @param newVar
	 *            the new var
	 */
	public void replaceVariableInProjection(String oldVar, String newVar) {
		if (this.addBinding == null) {
			this.addBinding = new HashMap<String, String>();
		}
		this.addBinding.put(oldVar, newVar);

		if (this.globalProjection != null) {
			this.globalProjection.replaceVariableInProjection(this.addBinding);
		}
	}

	/**
	 * Gets the adds the bindings.
	 * 
	 * @return the adds the bindings
	 */
	public HashMap<String, String> getAddBindings() {
		return this.addBinding;
	}
}
