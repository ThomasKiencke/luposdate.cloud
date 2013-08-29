/**
 * Copyright (c) 2013, Institute of Information Systems (Sven Groppe and contributors of LUPOSDATE), University of Luebeck
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
 * following conditions are met:
 *
 * 	- Redistributions of source code must retain the above copyright notice, this list of conditions and the following
 * 	  disclaimer.
 * 	- Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
 * 	  following disclaimer in the documentation and/or other materials provided with the distribution.
 * 	- Neither the name of the University of Luebeck nor the names of its contributors may be used to endorse or promote
 * 	  products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package lupos.cloud.optimizations.logical.rules.generated;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;

import lupos.cloud.operator.CloudSubgraphContainer;
import lupos.cloud.operator.MultiIndexScanContainer;
import lupos.cloud.operator.ICloudSubgraphExecutor;
import lupos.cloud.pig.operator.PigFilterOperator;
import lupos.cloud.storage.util.CloudManagement;
import lupos.datastructures.items.Variable;
import lupos.distributed.storage.distributionstrategy.TriplePatternNotSupportedError;
import lupos.engine.operators.BasicOperator;
import lupos.engine.operators.OperatorIDTuple;
import lupos.engine.operators.index.BasicIndexScan;
import lupos.engine.operators.index.Root;
import lupos.engine.operators.singleinput.AddBinding;
import lupos.engine.operators.singleinput.AddBindingFromOtherVar;
import lupos.engine.operators.singleinput.Projection;
import lupos.engine.operators.singleinput.Result;
import lupos.engine.operators.singleinput.filter.Filter;
import lupos.engine.operators.singleinput.modifiers.Limit;
import lupos.engine.operators.singleinput.modifiers.distinct.Distinct;
import lupos.optimizations.logical.rules.generated.runtime.Rule;

public class AddCloudSubGraphContainerRule extends Rule {

	public static CloudManagement cloudManagement;

	public static ICloudSubgraphExecutor subgraphExecutor;

	public HashSet<Variable> additionalProjectionVariables = new HashSet<Variable>();

	/**
	 * replace index scan operator with SubgraphContainer
	 * 
	 * @param indexScan
	 *            the index scan operator
	 */
	private void replaceIndexScanOperatorWithSubGraphContainer(
			final BasicOperator indexScan) {

		try {

			// Neuen Container erzeugen + inneren neuen rootNode
			final Root rootNodeOfOuterGraph = (Root) indexScan
					.getPrecedingOperators().get(0);

			// leere Liste einfügen, weil sonst NullpointerException - bug?
			rootNodeOfOuterGraph.setUnionVariables(new ArrayList<Variable>());

			final Root rootNodeOfSubGraph = rootNodeOfOuterGraph
					.newInstance(rootNodeOfOuterGraph.dataset);
			final CloudSubgraphContainer container = new CloudSubgraphContainer(
					rootNodeOfSubGraph, subgraphExecutor);
			final HashSet<Variable> variables = new HashSet<Variable>(
					indexScan.getIntersectionVariables());

			container.setUnionVariables(variables);
			container.setIntersectionVariables(variables);

			// alte Verbindungen merken
			final Collection<BasicOperator> preds = indexScan
					.getPrecedingOperators();
			List<OperatorIDTuple> succs = indexScan.getSucceedingOperators();
			for (final BasicOperator pred : preds) {
				pred.getOperatorIDTuple(indexScan).setOperator(container);
			}

			// Füge IndexscannOperator zum Caintainerhinzu und lösche alte
			// Nachfolger vom Indexscan
			rootNodeOfSubGraph.setSucceedingOperator(new OperatorIDTuple(
					indexScan, 0));
			indexScan.setSucceedingOperators(null);

			// Füge alle Nachfolger des IndexScannOps in eine Liste ein
			final LinkedHashSet<OperatorIDTuple> allSuccessors = getAllSuccessors(succs);

			// for (OperatorIDTuple oper : allSuccessors)
			// System.out.println("class: " +
			// oper.getOperator().getClass().toString());

			// Gehe die neue Liste durch und überprüfe ob Operatoren in den
			// SubGraphContainer verschoben werden können
			/*
			 * class lupos.engine.operators.singleinput.modifiers.Limit class
			 */
			for (OperatorIDTuple curOp : allSuccessors) {
				if ((curOp.getOperator() instanceof Filter
						&& checkIfFilterIsSupported((Filter) curOp
								.getOperator()) && checkIfFilterIsApplicableForIndexScan(
							rootNodeOfSubGraph, (Filter) curOp.getOperator(),
							indexScan.getUnionVariables()))
						|| curOp.getOperator() instanceof Projection
						|| curOp.getOperator() instanceof Distinct
						|| curOp.getOperator() instanceof Limit
						|| curOp.getOperator() instanceof AddBinding
						|| curOp.getOperator() instanceof AddBindingFromOtherVar
						|| curOp.getOperator() instanceof MultiIndexScanContainer) {
					/*
					 * Wenn ein direkter Nachfolger des Subgraphcontainer in den
					 * Container gezogen wird ist der neue Nachfolger des
					 * Containers, der alte Nachfolger vom verschobenen Operator
					 */
					if (succs.contains(curOp)) {
						succs = curOp.getOperator().getSucceedingOperators();
					}

					if (curOp.getOperator() instanceof Projection) {
						for (Variable var : additionalProjectionVariables) {
							((Projection) curOp.getOperator())
									.addProjectionElement(var);
						}
					}

					addToSubgraphContainerAndRemoveOldOperator(
							curOp.getOperator(), container, rootNodeOfSubGraph);
				}
			}

			// Füge Resultoperator hinzu
			getLastOperatorOfContainer(rootNodeOfSubGraph)
					.setSucceedingOperator(new OperatorIDTuple(new Result(), 0));

			// Container mit den Nachfolgern verbinden
			container.setSucceedingOperators(succs);

			// iterate through the new predecessors of the successors of the
			// original index scan operators and set new SubgraphContainer
			for (final OperatorIDTuple succ : succs) {
				succ.getOperator().removePrecedingOperator(indexScan);
				succ.getOperator().addPrecedingOperator(container);
			}

		} catch (final TriplePatternNotSupportedError e1) {
			System.err.println(e1);
			e1.printStackTrace();
		}
	}

	private boolean checkIfFilterIsApplicableForIndexScan(
			Root rootNodeOfSubgraph, Filter filter,
			Collection<Variable> unionVariables) {
		// if ("a".equals("a"))
		// return true;
		boolean result = true;
		for (String var : PigFilterOperator.getFilterVariables(filter
				.getNodePointer().getChildren()[0])) {
			if (!unionVariables.contains(var)) {
				result = false;
			}
		}

		if (result == false) {
			for (String var : PigFilterOperator.getFilterVariables(filter
					.getNodePointer().getChildren()[0])) {
				additionalProjectionVariables.add(new Variable(var.replace("?",
						"")));
			}

			System.out
					.println("Der Filter \""
							+ filter.toString().replace("\n", "")
							+ "\" wird nicht in der Cloud ausgeführt, da nicht alle Variablen im IndexScan-Operator vorhanden sind.");
		}
		return result;
	}

	private boolean checkIfFilterIsSupported(Filter filter) {
		boolean result = PigFilterOperator.checkIfFilterIsSupported(filter
				.getNodePointer().getChildren()[0]);
		if (result == false) {
			System.out
					.println("Der Filter \""
							+ filter.toString().replace("\n", "")
							+ "\" wird momentan nicht ünterstützt und deswegen lokal ausgeführt!");
		}

		return result;
	}

	private void addToSubgraphContainerAndRemoveOldOperator(
			BasicOperator operator, CloudSubgraphContainer container,
			Root rootNodeOfSubgraph) {
		Collection<BasicOperator> oldPreds = operator.getPrecedingOperators();
		List<OperatorIDTuple> oldSuccs = operator.getSucceedingOperators();

		BasicOperator lastOperation = getLastOperatorOfContainer(rootNodeOfSubgraph);

		operator.setSucceedingOperators(null);
		lastOperation.setSucceedingOperator(new OperatorIDTuple(operator, 0));

		for (final BasicOperator pred : oldPreds) {
			for (final OperatorIDTuple succ : oldSuccs) {
				pred.removePrecedingOperator(operator);
				pred.addPrecedingOperator(succ.getOperator());
			}
		}

		for (final OperatorIDTuple succ : oldSuccs) {
			for (final BasicOperator pred : oldPreds) {
				succ.getOperator().removePrecedingOperator(operator);
				succ.getOperator().addPrecedingOperator(pred);
			}
		}

	}

	private BasicOperator getLastOperatorOfContainer(BasicOperator operator) {
		BasicOperator result = null;
		if (operator.getSucceedingOperators() == null
				|| operator.getSucceedingOperators().size() == 0) {
			result = operator;
		} else {
			for (OperatorIDTuple elem : operator.getSucceedingOperators()) {
				result = getLastOperatorOfContainer(elem.getOperator());
			}
		}
		return result;
	}

	private LinkedHashSet<OperatorIDTuple> getAllSuccessors(
			List<OperatorIDTuple> succs) {
		final LinkedHashSet<OperatorIDTuple> allSuccessors = new LinkedHashSet<OperatorIDTuple>();
		ArrayList<OperatorIDTuple> justAdded = new ArrayList<OperatorIDTuple>();
		for (OperatorIDTuple elem : succs) {
			allSuccessors.add(elem);
			justAdded.add(elem);
		}
		for (OperatorIDTuple elem : justAdded) {
			for (OperatorIDTuple elemSucc : getAllSuccessors(elem.getOperator()
					.getSucceedingOperators())) {
				allSuccessors.add(elemSucc);
			}

		}

		return allSuccessors;
	}

	private BasicOperator currentOperator = null;

	private boolean _checkPrivate0(final BasicOperator _op) {
		if (!(_op instanceof BasicIndexScan || _op instanceof MultiIndexScanContainer)) {
			return false;
		}

		this.currentOperator = _op;

		return true;
	}

	public AddCloudSubGraphContainerRule() {
		this.startOpClass = lupos.engine.operators.index.BasicIndexScan.class;
		this.ruleName = "AddSubGraphContainer";
	}

	@Override
	protected boolean check(final BasicOperator _op) {
		return this._checkPrivate0(_op);
	}

	@Override
	protected void replace(
			final HashMap<Class<?>, HashSet<BasicOperator>> _startNodes) {
		this.replaceIndexScanOperatorWithSubGraphContainer(this.currentOperator);

	}
}
