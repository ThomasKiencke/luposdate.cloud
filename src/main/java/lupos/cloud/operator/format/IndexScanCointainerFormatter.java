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

import java.util.Collection;

import lupos.cloud.operator.IndexScanContainer;
import lupos.cloud.operator.format.IOperatorFormatter;
import lupos.cloud.pig.PigQuery;
import lupos.cloud.pig.operator.PigIndexScanOperator;
import lupos.engine.operators.BasicOperator;
import lupos.engine.operators.index.Root;
import lupos.engine.operators.singleinput.Projection;
import lupos.engine.operators.singleinput.Result;
import lupos.engine.operators.singleinput.filter.Filter;
import lupos.engine.operators.singleinput.modifiers.Limit;
import lupos.engine.operators.singleinput.modifiers.distinct.Distinct;
import lupos.engine.operators.tripleoperator.TriplePattern;

/**
 * Implements the formatter for the index scan operator
 */
public class IndexScanCointainerFormatter implements IOperatorFormatter {
	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * luposdate.operators.formatter.OperatorFormatter#serialize(lupos.engine
	 * .operators.BasicOperator, int)
	 */
	@Override
	public PigQuery serialize(final BasicOperator operator, PigQuery pigQuery) {
		PigQuery result = pigQuery;
		final IndexScanContainer indexScan = (IndexScanContainer) operator;
		Collection<TriplePattern> tp = indexScan.getIndexScan()
				.getTriplePattern();
		PigIndexScanOperator pigIndexScan = new PigIndexScanOperator(tp);
		pigQuery.buildAndAppendQuery(pigIndexScan);
		pigQuery.addIndexScanOperator(pigIndexScan);

		for (BasicOperator op : indexScan.getOperators()) {
			IOperatorFormatter serializer = null;
			if (op instanceof Filter) {
				serializer = new FilterFormatter();
			} else if (op instanceof Projection) {
				serializer = new ProjectionFormatter();
			} else if (op instanceof Distinct) {
				serializer = new DistinctFormatter();
			} else if (op instanceof Limit) {
				serializer = new LimitFormatter();
			} else if (op instanceof Result || op instanceof Root) {
				// ignore
			} else {
				throw new RuntimeException(
						"Something is wrong here. Forgot case? Class: "
								+ op.getClass());
			}

			if (serializer != null) {
				result = serializer.serialize(op, result);
			}
		}

		return pigQuery;
	}
}
