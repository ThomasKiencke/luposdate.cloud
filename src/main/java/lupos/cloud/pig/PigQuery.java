package lupos.cloud.pig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import lupos.cloud.operator.IndexScanContainer;
import lupos.cloud.operator.MultiIndexScanContainer;
import lupos.cloud.operator.format.DistinctFormatter;
import lupos.cloud.operator.format.FilterFormatter;
import lupos.cloud.operator.format.IndexScanCointainerFormatter;
import lupos.cloud.operator.format.LimitFormatter;
import lupos.cloud.operator.format.MultiIndexScanFormatter;
import lupos.cloud.operator.format.ProjectionFormatter;
import lupos.cloud.pig.operator.PigDistinctOperator;
import lupos.cloud.pig.operator.PigFilterOperator;
import lupos.cloud.pig.operator.PigIndexScanOperator;
import lupos.cloud.pig.operator.IPigOperator;
import lupos.cloud.pig.operator.PigJoinOperator;
import lupos.cloud.pig.operator.PigLimitOperator;
import lupos.cloud.pig.operator.PigProjectionOperator;
import lupos.cloud.pig.operator.PigUnionOperator;
import lupos.datastructures.items.Variable;
import lupos.engine.operators.BasicOperator;
import lupos.engine.operators.index.BasicIndexScan;
import lupos.engine.operators.index.Root;
import lupos.engine.operators.multiinput.Union;
import lupos.engine.operators.multiinput.join.Join;
import lupos.engine.operators.singleinput.AddBinding;
import lupos.engine.operators.singleinput.AddBindingFromOtherVar;
import lupos.engine.operators.singleinput.Projection;
import lupos.engine.operators.singleinput.Result;
import lupos.engine.operators.singleinput.filter.Filter;
import lupos.engine.operators.singleinput.modifiers.Limit;
import lupos.engine.operators.singleinput.modifiers.distinct.Distinct;

/**
 * In dieser Klassen werden Informationen über das PigQuery abgespeichert z.B.
 * das PigLatin Programm selbst sowie die zu erwartende Variablenliste.
 */
public class PigQuery {

	/** The pig latin. */
	StringBuilder pigLatin = new StringBuilder();

	// private PigProjectionOperator projection = null;

	/* Projektion die für den Container gültig ist */
	private PigProjectionOperator containerProjection = null;

	/* Filter die für den gesamten SubGraph Container gültig sind */
	private ArrayList<PigFilterOperator> containerFilterPigOps = new ArrayList<PigFilterOperator>();

	private ArrayList<PigIndexScanOperator> indexScanOps = new ArrayList<PigIndexScanOperator>();

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
			indexScanOps.get(0).addFilter(containerFilterPigOps);
			indexScanOps.get(0).addProjection(containerProjection);
			this.multiJoin(indexScanOps.get(0));
		} else {

			// push Filter/Projections
			executeFiltersAndProjections(containerProjection,
					containerFilterPigOps);

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
				if (op instanceof IndexScanContainer) {
					IndexScanContainer curIndexScanContainer = (IndexScanContainer) op;
					PigIndexScanOperator pigIndexScan = new PigIndexScanOperator(
							curIndexScanContainer.getIndexScan()
									.getTriplePattern());

					for (BasicOperator indexOp : curIndexScanContainer
							.getOperations()) {
						if (indexOp instanceof Filter) {
							PigFilterOperator filterOp = new PigFilterOperator(
									(Filter) indexOp);
							pigIndexScan.addFilter(filterOp);
						} else if (indexOp instanceof Projection) {
							PigProjectionOperator projectionOp = new PigProjectionOperator(
									((Projection) indexOp)
											.getProjectedVariables());
							pigIndexScan.setProjection(projectionOp);
						} else if (op instanceof Result) {
							// ignore
						} else {
							throw new RuntimeException(
									"Something is wrong here. Forgot case?");
						}
					}

					pigIndexScan.addFilter(containerFilterPigOps);
					pigIndexScan.addProjection(containerProjection);

					this.buildAndAppendQuery(pigIndexScan);
					
					this.addIndexScanOperator(pigIndexScan);
					
					this.multiJoin(pigIndexScan);
					multiInputist.add(intermediateBags.get(intermediateBags
							.size() - 1));
					
				} else if (op instanceof MultiIndexScanContainer) {
					final MultiIndexScanContainer c = (MultiIndexScanContainer) op;
					multiInputist.add(this.joinMultiIndexScans(c));
				}
			}

			newJoin = new JoinInformation();
			if (container.getMappingTree().get(id) instanceof Union) {
				this.buildAndAppendQuery(new PigUnionOperator(newJoin,
						multiInputist));
			} else if (container.getMappingTree().get(id) instanceof Join) {
				this.buildAndAppendQuery(new PigJoinOperator(newJoin,
						multiInputist, (Join) container.getMappingTree()
								.get(id)));
			} else {
				throw new RuntimeException(
						"Something is wrong here. Forgot case? -> "
								+ container.getMappingTree().get(id).getClass());
			}			

			HashSet<String> variables = new HashSet<String>();
			for (JoinInformation toRemove : multiInputist) {
				variables.addAll(toRemove.getJoinElements());
				this.intermediateBags.remove(toRemove);
			}
			
			newJoin.setJoinElements(new ArrayList<String>(variables));
			this.intermediateBags.add(newJoin);

		}
		return newJoin; // never reached
	}

	public void addMultiIndexScanList(MultiIndexScanContainer container) {
		// this.multiIndexScanList = multiIndexScanList;
		// this.mappingTree = mappingTree;
		this.multiIndexScanContainer = container;
	}

	public void addFilter(Integer indexScanId, PigFilterOperator pigFilter) {
		this.indexScanOps.get(indexScanId).addFilter(pigFilter);
	}

	public void setProjection(Integer indexScanID,
			PigProjectionOperator projection) {
		this.indexScanOps.get(indexScanID).setProjection(projection);
	}

	public void finishQuery() {

		executeFiltersAndProjections(containerProjection, containerFilterPigOps);

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

	private void executeFiltersAndProjections(PigProjectionOperator projection,
			ArrayList<PigFilterOperator> filterOps) {
		for (PigFilterOperator filter : filterOps) {
			this.buildAndAppendQuery(filter);
		}
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
	private void multiJoin(PigIndexScanOperator pigIndexScanOp) {
		this.activeIndexScan = pigIndexScanOp;
		// suche so lange bis es noch Mengen zum joinen gibt
		while (pigIndexScanOp.getIntermediateJoins().size() > 1) {
			/*
			 * Überprüfe bei jeden durchlauf ob eine Projektion/Filter
			 * durchgeführt werden kann (Grund: Projektion so früh wie möglich)
			 */

			// push filter/projection
			executeFiltersAndProjections(pigIndexScanOp.getProjection(),
					pigIndexScanOp.getFilter());

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
		}

		executeFiltersAndProjections(pigIndexScanOp.getProjection(),
				pigIndexScanOp.getFilter());

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
		this.indexScanOps.add(pigIndexScan);
	}

	public void setDistinctOperator(PigDistinctOperator pigDistinctOperator) {
		this.distinctOperator = pigDistinctOperator;
	}

	public void setLimitOperator(PigLimitOperator pigLimitOperator) {
		this.limitOperator = pigLimitOperator;
	}

	public ArrayList<PigFilterOperator> getFilterPigOps() {
		if (activeIndexScan == null) {
			return containerFilterPigOps;
		} else {
			return activeIndexScan.getFilter();
		}
	}

	public PigProjectionOperator getProjection() {
		if (activeIndexScan == null) {
			return containerProjection;
		} else {
			return activeIndexScan.getProjection();
		}
	}

	public void setContainerProjection(
			PigProjectionOperator pigProjectionOperator) {
		this.containerProjection = pigProjectionOperator;
	}

	public void addContainerFilter(PigFilterOperator pigFilter) {
		this.containerFilterPigOps.add(pigFilter);
	}
}
