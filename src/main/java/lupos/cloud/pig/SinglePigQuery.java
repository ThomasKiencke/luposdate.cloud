package lupos.cloud.pig;

import java.util.ArrayList;
import java.util.HashMap;

import lupos.cloud.bloomfilter.BitvectorManager;
import lupos.cloud.pig.operator.PigDistinctOperator;
import lupos.cloud.pig.operator.PigFilterExectuer;
import lupos.cloud.pig.operator.PigFilterOperator;
import lupos.cloud.pig.operator.PigIndexScanOperator;
import lupos.cloud.pig.operator.IPigOperator;
import lupos.cloud.pig.operator.PigLimitOperator;
import lupos.cloud.pig.operator.PigOrderByOperator;
import lupos.cloud.pig.operator.PigProjectionOperator;

/**
 * In dieser Klassen werden Informationen über das PigQuery abgespeichert z.B.
 * das PigLatin Programm selbst sowie die zu erwartende Variablenliste.
 */
public class SinglePigQuery {

	/** The pig latin. */
	StringBuilder pigLatin = new StringBuilder();
	
	BitvectorManager bitvectorManager = new BitvectorManager();

	/* Projektion die für den Container gültig ist */
	private PigProjectionOperator globalProjection = null;

	/* Filter die für den gesamten SubGraph Container gültig sind */
	private ArrayList<PigFilterOperator> globalFilterPigOp = new ArrayList<PigFilterOperator>();

	// private ArrayList<PigIndexScanOperator> indexScanOps = new
	// ArrayList<PigIndexScanOperator>();

	PigIndexScanOperator indexScanOp = null;

	/** The intermediate joins. */
	private ArrayList<JoinInformation> intermediateBags = new ArrayList<JoinInformation>();

	// private MultiIndexScanContainer multiIndexScanContainer = null;

	public boolean debug = true;

	private PigDistinctOperator distinctOperator = null;

	private PigLimitOperator limitOperator;

	private PigOrderByOperator pigOrderByOperator;
	
	private HashMap<String, String> addBinding = null;

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

	public ArrayList<JoinInformation> getIntermediateJoins() {
		return this.intermediateBags;
	}

	public boolean isDebug() {
		return debug;
	}

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

	public void buildAndAppendQuery(IPigOperator operator) {
		this.pigLatin.append(operator.buildQuery(this.intermediateBags, this.debug, this.globalFilterPigOp));
	}

	public void setIndexScanOperator(PigIndexScanOperator pigIndexScan) {
		this.indexScanOp = pigIndexScan;
	}

	public void setDistinctOperator(PigDistinctOperator pigDistinctOperator) {
		this.distinctOperator = pigDistinctOperator;
	}

	public void setLimitOperator(PigLimitOperator pigLimitOperator) {
		this.limitOperator = pigLimitOperator;
	}

	public ArrayList<PigFilterOperator> getFilterPigOps() {
		return globalFilterPigOp;
	}

	public PigProjectionOperator getProjection() {
		return globalProjection;
	}

	public void setContainerProjection(
			PigProjectionOperator pigProjectionOperator) {
		this.globalProjection = pigProjectionOperator;
	}

	public void addContainerFilter(PigFilterOperator pigFilter) {
		this.globalFilterPigOp.add(pigFilter);
	}

	public void addProjection(PigProjectionOperator newProjection) {
		if (globalProjection == null) {
			this.globalProjection = newProjection;
		} else {
			this.globalProjection.addProjectionVaribles(newProjection
					.getProjectionVariables());
		}
		this.globalProjection.replaceVariableInProjection(this.addBinding);
	}

	public void setOrderbyOperator(PigOrderByOperator pigOrderByOperator) {
		this.pigOrderByOperator = pigOrderByOperator;
	}

	public void replaceVariableInProjection(String oldVar, String newVar) {
		if (this.addBinding == null) {
			this.addBinding = new HashMap<String, String>();
		}
		this.addBinding.put(oldVar, newVar);
		
		if (this.globalProjection != null) {
			this.globalProjection.replaceVariableInProjection(this.addBinding);
		}
	}

	public  HashMap<String, String> getAddBindings() {
		return this.addBinding;
	}
	
	public BitvectorManager getBitvectorManager() {
		return bitvectorManager;
	}
}
