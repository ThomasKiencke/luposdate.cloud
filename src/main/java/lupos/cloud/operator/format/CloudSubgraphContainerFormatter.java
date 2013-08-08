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

import static com.google.common.base.Throwables.propagate;
import static com.google.common.collect.Lists.newLinkedList;
import static com.google.common.collect.Maps.newHashMap;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import lupos.cloud.operator.format.FilterFormatter;
import lupos.cloud.operator.format.IndexScanFormatter;
import lupos.cloud.operator.format.OperatorFormatter;
import lupos.cloud.operator.format.ResultFormatter;
import lupos.cloud.operator.format.RootFormatter;
import lupos.cloud.operator.format.CloudSubgraphContainerFormatter;
import lupos.cloud.operator.format.operatorcreator.IOperatorCreator;
import lupos.distributed.query.operator.withouthistogramsubmission.QueryClientIndexScan;
import lupos.distributed.query.operator.withouthistogramsubmission.QueryClientRoot;
import lupos.engine.operators.BasicOperator;
import lupos.engine.operators.OperatorIDTuple;
import lupos.engine.operators.application.Application;
import lupos.engine.operators.index.BasicIndexScan;
import lupos.engine.operators.index.Dataset;
import lupos.engine.operators.index.Root;
import lupos.engine.operators.singleinput.Result;
import lupos.engine.operators.singleinput.filter.Filter;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * The Class SubGraphContainerFormatter.
 */
public class CloudSubgraphContainerFormatter implements OperatorFormatter {

	private int id_counter;

	private Root root;

	private Dataset dataset;

	private Application application;

	private IOperatorCreator operatorCreator;

	public CloudSubgraphContainerFormatter() {
	}

	public CloudSubgraphContainerFormatter(final Dataset dataset,
			final IOperatorCreator operatorCreator,
			final Application application) {
		this.dataset = dataset;
		this.application = application;
		this.operatorCreator = operatorCreator;
	}

	@Override
	public String serialize(final BasicOperator operator, final int node_id) {

		StringBuilder pigLatin = new StringBuilder();

		this.serializeNode(new OperatorIDTuple(operator, 0), pigLatin);

		return pigLatin.toString();
	}

	private void serializeNode(final OperatorIDTuple node,
			StringBuilder pigLatin) {


		final BasicOperator op = node.getOperator();

		OperatorFormatter serializer;
		if (op instanceof BasicIndexScan) {
			serializer = new IndexScanFormatter();
		} else if (op instanceof Root) {
			serializer = new RootFormatter();
		} else if (op instanceof Result) {
			serializer = new ResultFormatter();
		} else if (op instanceof Filter) {
			serializer = new FilterFormatter();
		} else {
			throw new RuntimeException("Something is wrong here. Forgot case?");
		}

			pigLatin.append(serializer.serialize(op, this.id_counter) + "\n");

		for (final OperatorIDTuple successor : op.getSucceedingOperators()) {
			this.serializeNode(successor, pigLatin);
		}
	}

	@Override
	public Root deserialize(final JSONObject serializedOperator)
			throws JSONException {
		this.root = null;

		final HashMap<Integer, BasicOperator> nodes = this
				.deserializeNodes(serializedOperator);

		final JSONArray edgesJson = (JSONArray) serializedOperator.get("edges");
		CloudSubgraphContainerFormatter.deserializeEdges(edgesJson, nodes);

		return this.root;
	}

	private static void deserializeEdges(final JSONArray edgesJson,
			final HashMap<Integer, BasicOperator> nodes) throws JSONException {

		final HashMap<BasicOperator, List<OperatorIDTuple>> succeedingOperators = newHashMap();
		final HashMap<BasicOperator, List<BasicOperator>> precedingOperators = newHashMap();

		for (int i = 0; i < edgesJson.length(); i++) {

			final JSONObject edgeJson = edgesJson.getJSONObject(i);

			final BasicOperator from = nodes.get(edgeJson.getInt("from"));
			final BasicOperator to = nodes.get(edgeJson.getInt("to"));

			if (succeedingOperators.get(from) == null) {
				succeedingOperators
						.put(from, new LinkedList<OperatorIDTuple>());
			}

			if (precedingOperators.get(to) == null) {
				precedingOperators.put(to, new LinkedList<BasicOperator>());
			}

			succeedingOperators.get(from).add(
					new OperatorIDTuple(to, edgeJson.getInt("edge_id")));
			precedingOperators.get(to).add(from);
		}

		for (final Entry<BasicOperator, List<OperatorIDTuple>> from : succeedingOperators
				.entrySet()) {
			from.getKey().setSucceedingOperators(from.getValue());
		}

		for (final Entry<BasicOperator, List<BasicOperator>> to : precedingOperators
				.entrySet()) {
			to.getKey().setPrecedingOperators(to.getValue());
		}
	}

	private HashMap<Integer, BasicOperator> deserializeNodes(
			final JSONObject rootJson) throws JSONException {

		final HashMap<Integer, BasicOperator> nodes = newHashMap();
		final JSONArray nodesJson = (JSONArray) rootJson.get("nodes");

		final HashMap<String, OperatorFormatter> formatters = this
				.createFormatters();

		for (int i = 0; i < nodesJson.length(); i++) {

			final JSONObject nodeJson = nodesJson.getJSONObject(i);

			// get corresponding formatter from map
			final OperatorFormatter formatter = formatters.get(nodeJson
					.getString("type"));

			// add deserialized node to list
			final BasicOperator node = formatter.deserialize(nodeJson);
			nodes.put(nodeJson.getInt("node_id"), node);

			if (node instanceof Root) {
				final IndexScanFormatter indexScanFormatter = (IndexScanFormatter) formatters
						.get(BasicIndexScan.class.getName());
				indexScanFormatter.setRoot((Root) node);
			}

			try {
				if (nodeJson.getBoolean("root")) {
					this.root = (Root) node;
				}
			} catch (final JSONException e) {
				// ignore
			}
		}

		return nodes;
	}

	private HashMap<String, OperatorFormatter> createFormatters() {
		final HashMap<String, OperatorFormatter> formatters = newHashMap();
		formatters.put(Root.class.getName(), new RootFormatter(this.dataset,
				this.operatorCreator));
		formatters.put(BasicIndexScan.class.getName(), new IndexScanFormatter(
				this.operatorCreator));
		formatters.put(QueryClientRoot.class.getName(), new RootFormatter(
				this.dataset, this.operatorCreator));
		formatters.put(QueryClientIndexScan.class.getName(),
				new IndexScanFormatter(this.operatorCreator));
		formatters.put(Filter.class.getName(), new FilterFormatter());
		formatters.put(Result.class.getName(), new ResultFormatter(
				this.application));
		return formatters;
	}
}