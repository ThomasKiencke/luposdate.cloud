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
package lupos.cloud.optimizations.logical.rules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import lupos.cloud.operator.IndexScanContainer;
import lupos.cloud.operator.MultiIndexScanContainer;
import lupos.cloud.operator.ICloudSubgraphExecutor;
import lupos.cloud.pig.operator.PigFilterOperator;
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
import lupos.engine.operators.singleinput.Result;
import lupos.engine.operators.singleinput.filter.Filter;
import lupos.optimizations.logical.rules.generated.runtime.Rule;

public class AddMergeContainerRule extends Rule {

	public static CloudManagement cloudManagement;

	public static ICloudSubgraphExecutor subgraphExecutor;
	ArrayList<BasicOperator> containerList;

	public static boolean finish = false;

	private void replaceIndexScanOperatorWithSubGraphContainer(
			QueryClientRoot qcRoot) {

		// Am Anfang werden alle IndexScansContainer und MultiIndexScanContainer
		// in die Liste gepackt
		containerList = new ArrayList<BasicOperator>();
		for (OperatorIDTuple op : qcRoot.getSucceedingOperators()) {
			if (op.getOperator() instanceof IndexScanContainer
					|| op.getOperator() instanceof MultiIndexScanContainer) {
				containerList.add(op.getOperator());
			}
		}

		// Die IndexScans werden nun so lange gemerged bis nur noch ein
		// Container existiert

		if (containerList.size() == 1) {
			finish = true;
		} else {
			mergeContainer();
		}
	}

	private void mergeContainer() {
		HashMap<BasicOperator, HashSet<BasicOperator>> mergeMap = new HashMap<BasicOperator, HashSet<BasicOperator>>();

		// Für jeden (Multi-)Index-Container wird die Nachfolge MultiInput
		// Operation gesucht und in die mergeMap gepackt. Dort befinet sich
		// danach die MultiInputOperation also z.B. Union und eine Lister der
		// beteiligten (Multi-)IndexScan Operationen
		for (BasicOperator op : containerList) {
			for (OperatorIDTuple path : op.getSucceedingOperators()) {
				BasicOperator foundOp = OperatorGraphHelper
						.getNextMultiInputOperation(path.getOperator());
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
		}

		// Für jeden MultiInputOperator wird nun ein eigner Container erstellt
		// mit allen dazugehörigen (Multi-)Index-Scan Containern
		for (BasicOperator multiInputOperator : mergeMap.keySet()) {
			HashSet<BasicOperator> toMerge = mergeMap.get(multiInputOperator);
			// Eine MultiInput Operator braucht immer mehr als eine Input
			// Operation
			if (toMerge.size() > 1) {
				MultiIndexScanContainer multiIndexContainer = new MultiIndexScanContainer();

				// Füge Union/Intersection-Variablen hinzu
				multiIndexContainer.setUnionVariables(OperatorGraphHelper
						.getUnionVariablesFromMultipleOperations(toMerge));
				multiIndexContainer
						.setIntersectionVariables(OperatorGraphHelper
								.getIntersectionVariablesFromMultipleOperations(toMerge));

				// Neuen Container erzeugen und den MultiInput Operator und
				// (Multi-)IndexScans übergeben
				multiIndexContainer.addSubContainer(
						(MultiInputOperator) multiInputOperator, toMerge);

				// Wenn der MultiInputOperator von Variablen abhängt
				// müssen die als Projektion in den Containern
				// hinzugefügt werden
				OperatorGraphHelper
						.addProjectionFromMultiInputOperatorInContainerIfNecessary(
								multiInputOperator, toMerge);

				// Füge Operatoren zum Container hinzu
				for (BasicOperator op : OperatorGraphHelper
						.getAndDeleteOperationUntilNextMultiInputOperator(multiInputOperator
								.getSucceedingOperators())) {
					if (OperatorGraphHelper.isOperationSupported(op)) {
						// Wenn die Operation unterstützt wird füge zum
						// Container hinzu
						multiIndexContainer.addOperator(op);
						// Falls eine Operatione z.B. eine Projektion/Filter von
						// Variablen abhängig ist füge diese zur inneren Container-
						// Projektion hinzu
						OperatorGraphHelper.addProjectionIfNecessary(op, containerList);
					} else {
						// Ansonsten hänge die Operation hinter den Container
						OperatorGraphHelper.insertNewOperator(
								multiIndexContainer, op);
					}
				}
				
				// Entferne den Container aus der Container Liste, wenn dieser
				// nicht noch für eine andere MultiInput-Operation gebraucht
				// wird.
				HashSet<BasicOperator> toRemove = new HashSet<BasicOperator>();
				for (BasicOperator container : toMerge) {
					OperatorGraphHelper.removeDuplicatedEdges(container);
					if (container.getSucceedingOperators().size() == 1) {
						containerList.remove(container);
						toRemove.add(container);
					} else {
						multiInputOperator.removePrecedingOperator(container);
						container.removeSucceedingOperator(multiInputOperator);
					}
				}

				OperatorGraphHelper.mergeContainerListIntoOneNewContainer(
						multiIndexContainer, toRemove);


				multiInputOperator.removeFromOperatorGraph();

				containerList.add(multiIndexContainer);

			}
		}

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
				return !finish;
			}
		}
	}

	public AddMergeContainerRule() {
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
