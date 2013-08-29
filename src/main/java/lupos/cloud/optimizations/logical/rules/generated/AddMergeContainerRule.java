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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import lupos.cloud.operator.IndexScanContainer;
import lupos.cloud.operator.MultiIndexScanContainer;
import lupos.cloud.operator.ICloudSubgraphExecutor;
import lupos.cloud.storage.util.CloudManagement;
import lupos.datastructures.items.Variable;
import lupos.distributed.query.operator.withouthistogramsubmission.QueryClientRoot;
import lupos.engine.operators.BasicOperator;
import lupos.engine.operators.OperatorIDTuple;
import lupos.engine.operators.index.BasicIndexScan;
import lupos.engine.operators.multiinput.MultiInputOperator;
import lupos.engine.operators.multiinput.Union;
import lupos.engine.operators.multiinput.join.Join;
import lupos.engine.operators.multiinput.optional.Optional;
import lupos.engine.operators.singleinput.Projection;
import lupos.optimizations.logical.rules.generated.runtime.Rule;

public class AddMergeContainerRule extends Rule {

	public static CloudManagement cloudManagement;

	public static ICloudSubgraphExecutor subgraphExecutor;
	ArrayList<BasicOperator> multiInputList;

	private void replaceIndexScanOperatorWithSubGraphContainer(
			QueryClientRoot qcRoot) {

		// Am Anfang werden alle IndexScans in die Liste gepackt
		multiInputList = new ArrayList<BasicOperator>();
		for (OperatorIDTuple op : qcRoot.getSucceedingOperators()) {
			if (op.getOperator() instanceof IndexScanContainer) {
				multiInputList.add(op.getOperator());
			}
		}

		// Die IndexScans werden nun so lange gemerged bis nur noch ein
		// Container existiert
		while (multiInputList.size() > 1) {
			mergeContainer();
		}
	}

	private void mergeContainer() {
		HashMap<BasicOperator, HashSet<BasicOperator>> mergeMap = new HashMap<BasicOperator, HashSet<BasicOperator>>();
		// Class[] mergeClasses = { Union.class, Optional.class, Join.class };
		for (BasicOperator op : multiInputList) {
			// Suche MultiInput Klasse und gib diese zurück
			BasicOperator foundOp = this.getOperator(op);
			if (foundOp != null) {
				HashSet<BasicOperator> list = mergeMap.get(foundOp);
				if (list == null) {
					list = new HashSet<BasicOperator>();
					list.add(op);
					mergeMap.put(foundOp, list);
				} else {
					list.add(op);
				}
			}
		}

		// merge
		for (BasicOperator op : mergeMap.keySet()) {
			HashSet<BasicOperator> toMerge = mergeMap.get(op);
			if (toMerge.size() > 1) {

				MultiIndexScanContainer multiIndexContainer = new MultiIndexScanContainer();

				// leere Liste einfügen, weil sonst NullpointerException
				multiIndexContainer
						.setUnionVariables(new ArrayList<Variable>());
				multiIndexContainer
						.setIntersectionVariables(new ArrayList<Variable>());

				OperatorIDTuple multiIndexScanContainerOpID = new OperatorIDTuple(
						multiIndexContainer, 0);

				multiIndexContainer.addOperator((MultiInputOperator) op,
						toMerge);

				// Wenn der MultiInputContainer von Variablen abhängig ist
				// müssen die als Projektion in den IndexScanContainer
				// hinzugefügt werden
				ArrayList<Variable> intersectionVariables = new ArrayList<Variable>(
						op.getIntersectionVariables());
				if (intersectionVariables.size() > 0) {
					Projection proj = new Projection();
					for (Variable var : intersectionVariables) {
						proj.addProjectionElement(var);
					}
					for (BasicOperator indexScan : toMerge) {
						if (indexScan instanceof IndexScanContainer) {
							((IndexScanContainer) indexScan).addOperator(proj);
						}
						else {
							((MultiIndexScanContainer) indexScan).addOperatorToAllChilds(proj);
						}
					}
				}

				for (BasicOperator indexScan : toMerge) {
					multiInputList.remove(indexScan);
				}

				this.insertAndDeleteOldConnections(multiIndexScanContainerOpID,
						toMerge);

				op.removeFromOperatorGraph();

				multiInputList.add(multiIndexContainer);

			}
		}

	}

	private void insertAndDeleteOldConnections(OperatorIDTuple newOp,
			HashSet<BasicOperator> oldOperators) {
		// lösche alte IndexScans und füge neue Verbindungen ein
		HashSet<BasicOperator> preds = new HashSet<BasicOperator>();
		HashSet<OperatorIDTuple> succs = new HashSet<OperatorIDTuple>();

		// Finde Vorgänger/Nachfolger ALLER Operatoren
		for (BasicOperator old : oldOperators) {
			preds.addAll(old.getPrecedingOperators());
			succs.addAll(old.getSucceedingOperators());
			old.removeFromOperatorGraph();
		}

		// Für jeden Vorgänger den neuen Nachfolger setezen
		for (BasicOperator pred : preds) {
			pred.addSucceedingOperator(newOp);
			ArrayList<OperatorIDTuple> toRemvoe = new ArrayList<OperatorIDTuple>();

			// Beim löschen der neuen Operationen werden alle Vorgänger mit den
			// Nachfolgern verbunden, diese Verbindung muss gelöscht werden
			for (OperatorIDTuple succ : succs) {
				for (OperatorIDTuple op : pred.getSucceedingOperators())
					if (op.getOperator().equals(succ.getOperator())) {
						toRemvoe.add(op);
					}
			}
			for (OperatorIDTuple op : toRemvoe) {
				pred.getSucceedingOperators().remove(op);
			}
		}

		// Für die neue Operation die alten Vorgänger setzen
		newOp.getOperator().setPrecedingOperators(
				new LinkedList<BasicOperator>(preds));

		// Für jeden Nachfolger den neuen Vorgänger setzen
		int i = 0;
		for (OperatorIDTuple succ : succs) {
			if (i == 0) {
				succ.getOperator().setPrecedingOperator(newOp.getOperator());
			} else {
				succ.getOperator().addPrecedingOperator(newOp.getOperator());
			}
			i++;
		}

		// Für die neue Operation die alten Nachfolger setzen
		newOp.getOperator().setSucceedingOperators(
				new LinkedList<OperatorIDTuple>(succs));

	}

	private BasicOperator getOperator(BasicOperator startOp) {
		BasicOperator result = null;
		List<OperatorIDTuple> succs = startOp.getSucceedingOperators();
		if (succs == null) {
			return null;
		} else {
			for (OperatorIDTuple node : succs) {
				if (node.getOperator() instanceof MultiInputOperator) {
					return node.getOperator();
				}
				return this.getOperator(node.getOperator());
			}

		}
		return result;
	}

	private QueryClientRoot indexScan = null;

	private boolean _checkPrivate0(final BasicOperator _op) {

		// workaround, nicht schön - aber funktioniert :)
		if (!(_op instanceof QueryClientRoot)) {
			return false;
		} else {
			if (indexScan == null) {
				this.indexScan = (QueryClientRoot) _op;
				return true;
			} else {
				return false;
			}
		}
	}

	public AddMergeContainerRule() {
		// this.startOpClass =
		// lupos.engine.operators.index.BasicIndexScan.class;
		// this.startOpClass =
		// lupos.engine.operators.index.BasicIndexScan.class;
		this.startOpClass = QueryClientRoot.class;
		this.ruleName = "AddMergeContainer";
	}

	@Override
	protected boolean check(final BasicOperator _op) {
		return this._checkPrivate0(_op);
	}

	@Override
	protected void replace(
			final HashMap<Class<?>, HashSet<BasicOperator>> _startNodes) {
		this.replaceIndexScanOperatorWithSubGraphContainer(this.indexScan);

	}
}
