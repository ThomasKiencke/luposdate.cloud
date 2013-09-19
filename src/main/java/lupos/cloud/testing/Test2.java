package lupos.cloud.testing;

import java.util.LinkedList;

import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler;
import org.apache.pig.builtin.Bloom;
import org.apache.pig.builtin.BuildBloom;
import org.apache.pig.builtin.JsonMetadata;

import lupos.datastructures.items.literal.LiteralFactory;
import lupos.datastructures.items.literal.URILiteral;
import lupos.datastructures.queryresult.QueryResult;
import lupos.engine.evaluators.BasicIndexQueryEvaluator;
import lupos.engine.evaluators.MemoryIndexQueryEvaluator;

public class Test2 {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		String input = "@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n @prefix bench: <http://localhost/vocabulary/bench/> .\n <http://localhost/publications/articles/Journal1/1940/Article16> rdf:type bench:Article.\n";
		MemoryIndexQueryEvaluator memoryEvaluator = new MemoryIndexQueryEvaluator();
		loadInMemory(input, memoryEvaluator);
		QueryResult result = executeQuery(memoryEvaluator,
				"SELECT DISTINCT ?s WHERE {?s ?p ?o .}");

		System.out.println("result:  " + result);
		
		BuildBloom a;
		Bloom b;
		JsonMetadata c;
		JobControlCompiler d;
		SingleColumnValueFilter asd;
//		org.apache.hadoop.hbase.filter.FuzzyRowFilter

	}

	private static void loadInMemory(String input,
			MemoryIndexQueryEvaluator memoryEvaluator)
			throws Exception {
		final URILiteral rdfURL = LiteralFactory
				.createStringURILiteral("<inlinedata:" + input + ">");
		LinkedList<URILiteral> defaultGraphs = new LinkedList<URILiteral>();
		defaultGraphs.add(rdfURL);

		memoryEvaluator.prepareInputData(defaultGraphs,
				new LinkedList<URILiteral>());
	}


	protected static QueryResult executeQuery(
			BasicIndexQueryEvaluator evaluator, String query) throws Exception {
		evaluator.compileQuery(query);
		evaluator.logicalOptimization();
		evaluator.physicalOptimization();

		return evaluator.getResult();
	}

}