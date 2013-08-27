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
package lupos.cloud.operator.format;

import lupos.cloud.operator.format.FilterFormatter;
import lupos.cloud.operator.format.IndexScanFormatter;
import lupos.cloud.operator.format.IOperatorFormatter;
import lupos.cloud.operator.format.CloudSubgraphContainerFormatter;
import lupos.cloud.pig.PigQuery;
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

/**
 * The Class https://repository.cloudera.com/artifactory/cloudera-repos.
 */
public class CloudSubgraphContainerFormatter implements IOperatorFormatter {

	public CloudSubgraphContainerFormatter() {
	}

	@Override
	public PigQuery serialize(final BasicOperator operator, PigQuery pigLatin) {
		PigQuery result = this.serializeNode(new OperatorIDTuple(operator, 0),
				pigLatin);
		pigLatin.applyJoins();
		pigLatin.finishQuery();
		return result;
	}

	private PigQuery serializeNode(final OperatorIDTuple node, PigQuery pigLatin) {

		PigQuery result = null;
		final BasicOperator op = node.getOperator();

		IOperatorFormatter serializer = null;
		if (op instanceof BasicIndexScan) {
			serializer = new IndexScanFormatter();
		} else if (op instanceof Root) {
			result = pigLatin;
		} else if (op instanceof Filter) {
			serializer = new FilterFormatter();
		} else if (op instanceof Projection || op instanceof AddCloudProjection) {
			serializer = new ProjectionFormatter();
		} else if (op instanceof Distinct) {
			serializer = new DistinctFormatter();
		} else if (op instanceof Limit) {
			serializer = new LimitFormatter();
		} else if (op instanceof AddBinding
				|| op instanceof AddBindingFromOtherVar || op instanceof Result) {
			// ignore
			result = pigLatin;
		}  else {
			throw new RuntimeException("Something is wrong here. Forgot case?");
		}

		if (serializer != null) {
			result = serializer.serialize(op, pigLatin);
		}

		for (final OperatorIDTuple successor : op.getSucceedingOperators()) {
			result = this.serializeNode(successor, result);
		}

		return result;
	}
}