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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import lupos.cloud.operator.IndexScanContainer;
import lupos.cloud.operator.ICloudSubgraphExecutor;
import lupos.cloud.pig.operator.PigFilterOperator;
import lupos.cloud.storage.util.CloudManagement;
import lupos.datastructures.items.Variable;
import lupos.engine.operators.BasicOperator;
import lupos.engine.operators.OperatorIDTuple;
import lupos.engine.operators.index.BasicIndexScan;
import lupos.engine.operators.multiinput.MultiInputOperator;
import lupos.engine.operators.singleinput.Result;
import lupos.engine.operators.singleinput.filter.Filter;
import lupos.optimizations.logical.rules.generated.runtime.Rule;

public class AddIndexScanContainerRule extends Rule {

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
			final BasicIndexScan indexScan) {

		// Container erzeugen und Variablen übernehmen
		final IndexScanContainer container = new IndexScanContainer(indexScan);

		container.setUnionVariables(indexScan.getUnionVariables());
		container
				.setIntersectionVariables(indexScan.getIntersectionVariables());

		// Die zum IndexScanOperator zugehörigen Operationen werden mit in den
		// Container kopiert (wenn die Operation in der Cloud unterstützt wird)
		boolean oneOperationsWasNotSupported = false;
		for (BasicOperator op : OperatorGraphHelper
				.getAndDeleteOperationUntilNextMultiInputOperator(indexScan
						.getSucceedingOperators())) {
			if (OperatorGraphHelper.isOperationSupported(op)
					&& !oneOperationsWasNotSupported) {
				container.addOperator(op);
			} else {
				// Ansonsten hänge die Operation hinter den Container.
				// Alle Folgeoperationen werden dann, obwohl sie
				// vielleichtsogar unterstützt wreden, auch dahitner
				// gehängt, weil
				// sonst die Reihenfolge durcheinander gebracht werden
				// würde
				OperatorGraphHelper.insertNewOperator(OperatorGraphHelper.getLastOperator(container), op);
				oneOperationsWasNotSupported = true;
			}
		}

		// Ersetze den alten IndexScanOperator durch den neuen
		// IndexScanContainer
		OperatorGraphHelper.replaceOperation(indexScan, container);

	}

	private BasicIndexScan currentOperator = null;

	private boolean _checkPrivate0(final BasicOperator _op) {
		if (!(_op instanceof BasicIndexScan)) {
			return false;
		}

		this.currentOperator = (BasicIndexScan) _op;

		return true;
	}

	public AddIndexScanContainerRule() {
		this.startOpClass = lupos.engine.operators.index.BasicIndexScan.class;
		this.ruleName = "AddIndexScanContainerRule";
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
