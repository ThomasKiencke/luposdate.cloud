package lupos.cloud.pig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import lupos.cloud.pig.operator.IPigOperator;
import lupos.cloud.pig.operator.PigDistinctOperator;
import lupos.cloud.pig.operator.PigFilterExectuer;
import lupos.cloud.pig.operator.PigFilterOperator;
import lupos.cloud.pig.operator.PigJoinOperator;
import lupos.cloud.pig.operator.PigLimitOperator;
import lupos.cloud.pig.operator.PigOrderByOperator;
import lupos.cloud.pig.operator.PigProjectionOperator;
import lupos.cloud.testing.CloudBitvector;
import lupos.engine.operators.BasicOperator;
import lupos.engine.operators.index.Root;
import lupos.engine.operators.singleinput.AddBinding;
import lupos.engine.operators.singleinput.Projection;
import lupos.engine.operators.singleinput.Result;
import lupos.engine.operators.singleinput.filter.Filter;
import lupos.engine.operators.singleinput.modifiers.Limit;
import lupos.engine.operators.singleinput.modifiers.distinct.Distinct;
import lupos.engine.operators.singleinput.sort.Sort;

public class PigQuery {

	ArrayList<SinglePigQuery> singleQueries = new ArrayList<SinglePigQuery>();
	ArrayList<JoinInformation> intermediateBags = new ArrayList<JoinInformation>();
	StringBuilder pigLatin = new StringBuilder();
	public static boolean debug = true;
	private HashMap<String, String> addBinding = new HashMap<String, String>();

	public void finishQuery() {
		StringBuilder modifiedPigQuery = new StringBuilder();
		modifiedPigQuery.append(this.pigLatin.toString().replace(
				this.getFinalAlias(), "X"));
		this.pigLatin = modifiedPigQuery;
	}

	public String getPigLatin() {
		return pigLatin.toString();
	}

	public ArrayList<String> getVariableList() {
		ArrayList<String> result = new ArrayList<String>();
		for (String elem : intermediateBags.get(0).getJoinElements()) {
			// Bindings werden für die Ergebnismenge wieder rückgängig gemacht
			boolean replaced = false;
			for (String oldVar : this.addBinding.keySet()) {
				if (elem.equals(this.addBinding.get(oldVar))) {
					result.add(oldVar.replace("?", ""));
					replaced = true;
				}

			}
			if (!replaced) {
				result.add(elem.replace("?", ""));
			}

		}
		return result;
	}

	public void addAndPrceedSinglePigQuery(SinglePigQuery singlePigQuery) {
		singlePigQuery.finishQuery();
		this.singleQueries.add(singlePigQuery);
		if (singlePigQuery.getAddBindings() != null) {
			this.addBinding.putAll(singlePigQuery.getAddBindings());
		}
		intermediateBags.add(singlePigQuery.getIntermediateJoins().get(0));
		pigLatin.append(singlePigQuery.getPigLatin());
	}

	public void removeIntermediateBags(JoinInformation toRemove) {
		this.intermediateBags.remove(toRemove);
	}

	public void addIntermediateBags(JoinInformation newJoin) {
		this.intermediateBags.add(newJoin);
	}

	public JoinInformation getLastAddedBag() {
		JoinInformation result = null;
		result = intermediateBags.get(intermediateBags.size() - 1);
		return result;
	}

	public String getFinalAlias() {
		return intermediateBags.get(0).getName();
	}

	public void addAndExecuteOperation(ArrayList<BasicOperator> oplist) {
		ArrayList<PigFilterOperator> filterOps = new ArrayList<PigFilterOperator>();
		PigProjectionOperator projection = null;
		PigDistinctOperator distinct = null;
		PigLimitOperator limit = null;
		PigOrderByOperator orderBy = null;

		for (BasicOperator op : oplist) {
			if (op instanceof Filter) {
				filterOps.add(new PigFilterOperator((Filter) op));
			} else if (op instanceof Projection) {
				projection = new PigProjectionOperator(
						((Projection) op).getProjectedVariables());
			} else if (op instanceof Distinct) {
				distinct = new PigDistinctOperator();
			} else if (op instanceof Sort) {
				orderBy = new PigOrderByOperator(((Sort) op));
			} else if (op instanceof Limit) {
				limit = new PigLimitOperator(((Limit) op).getLimit());
			} else if (op instanceof Result || op instanceof Root
					|| op instanceof AddBinding) {
				// ignore
			} else {
				throw new RuntimeException(
						"Something is wrong here. Forgot case? Class: "
								+ op.getClass());
			}
		}

		// Filter
		if (filterOps.size() > 0) {
			this.buildAndAppendQuery(new PigFilterExectuer(), filterOps);
		}

		// Limit
		if (limit != null) {
			this.buildAndAppendQuery(limit, filterOps);
		}

		// Order by
		if (orderBy != null) {
			this.buildAndAppendQuery(orderBy, filterOps);
		}

		// Projection
		if (projection != null) {
			if (this.addBinding.size() > 0) {
				projection.replaceVariableInProjection(this.addBinding);
			}
			this.buildAndAppendQuery(projection, filterOps);
		}

		// Distinct
		if (distinct != null) {
			this.buildAndAppendQuery(distinct, filterOps);
		}

	}

	public void buildAndAppendQuery(IPigOperator operator,
			ArrayList<PigFilterOperator> filterOps) {
		this.pigLatin.append(operator.buildQuery(intermediateBags, debug,
				filterOps));
	}

	public void buildAndAppendQuery(IPigOperator operator) {
		this.pigLatin.append(operator.buildQuery(intermediateBags, debug,
				new ArrayList<PigFilterOperator>()));
	}

	public void replaceVariableInProjection(String oldVar, String newVar) {
		if (this.addBinding == null) {
			this.addBinding = new HashMap<String, String>();
		}
		this.addBinding.put(oldVar, newVar);
	}

	public void append(String toAdd) {
		this.pigLatin.append(toAdd);
	}

	public HashMap<String, HashSet<CloudBitvector>> getBitvectors() {
		return this.intermediateBags.get(0).getBitVectors();
	}
}
