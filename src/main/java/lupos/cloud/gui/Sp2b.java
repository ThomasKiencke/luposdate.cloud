/**
 * Copyright (c) 2012, Institute of Telematics, Institute of Information Systems (Dennis Pfisterer, Sven Groppe, Andreas Haller, Thomas Kiencke, Sebastian Walther, Mario David), University of Luebeck
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
package lupos.cloud.gui;

import java.io.InputStream;
import java.util.LinkedList;

import lupos.cloud.query.CloudEvaluator;
import lupos.datastructures.items.literal.LiteralFactory;
import lupos.datastructures.items.literal.URILiteral;
import lupos.datastructures.queryresult.QueryResult;
import lupos.engine.evaluators.BasicIndexQueryEvaluator;
import lupos.engine.evaluators.QueryEvaluator;
import lupos.sparql1_1.Node;

import org.junit.Ignore;
import org.junit.Test;

/**
 * Führt den SP2B Test aus und gibt die Zeit zurück.
 */
public class Sp2b {

	protected static final String q1_query_filename = "sp2b/q1.sparql";
	protected static final String q2_query_filename = "sp2b/q2.sparql";
	protected static final String q3a_query_filename = "sp2b/q3a.sparql";
	protected static final String q3b_query_filename = "sp2b/q3b.sparql";
	protected static final String q3c_query_filename = "sp2b/q3c.sparql";
	protected static final String q4_query_filename = "sp2b/q4.sparql";
	protected static final String q5a_query_filename = "sp2b/q5a.sparql";
	protected static final String q5b_query_filename = "sp2b/q5b.sparql";
	protected static final String q6_query_filename = "sp2b/q6.sparql";
	protected static final String q7_query_filename = "sp2b/q7.sparql";
	protected static final String q8_query_filename = "sp2b/q8.sparql";
	protected static final String q9_query_filename = "sp2b/q9.sparql";
	protected static final String q10_query_filename = "sp2b/q10.sparql";
	protected static final String q11_query_filename = "sp2b/q11.sparql";
	protected static final String q12a_query_filename = "sp2b/q12a.sparql";
	protected static final String q12b_query_filename = "sp2b/q12b.sparql";
	protected static final String q12c_query_filename = "sp2b/q12c.sparql";

	private static CloudEvaluator cloudEvaluator;
	private static double[] result_time = new double[17];
	private static double[] result_queryresult = new double[17];
	
	public static void main(String[] args) throws Exception {

		cloudEvaluator = new CloudEvaluator();

		// Tests ausführen:
		try {
			System.out.println("Tests werden ausgeführt:");
			testQ1();
			testQ2();
			testQ3a();
			testQ3b();
			testQ3c();
			testQ4();
			testQ5a();
			testQ5b();
			// testQ6(); // not supported yet
			testQ7();
			// testQ8(); // not supported yet
			testQ9();
			testQ10();
			testQ11();
			// testQ12a(); // geht zwar, aber ist die selbe Anfrage wie 5a
			// testQ12b(); // not supported yet
			// testQ12c(); // not supported yet

			printCSV();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void printCSV() {
		System.out.println("CSV Format: ");
		int i = 0;
		for (double result : result_time) {
			System.out.println(result + "\t" + result_queryresult[i]);
			i++;
		}
	}

	protected static String readFile(String file) {
		InputStream is = Sp2b.class.getClassLoader().getResourceAsStream(file);
		String selectQuery = convertStreamToString(is);
		return selectQuery;
	}

	private static String convertStreamToString(java.io.InputStream is) {
		java.util.Scanner s = new java.util.Scanner(is).useDelimiter("\\A");
		return s.hasNext() ? s.next() : "";
	}

	protected static QueryResult executeQuery(
			BasicIndexQueryEvaluator evaluator, String query) throws Exception {
		evaluator.prepareInputData(new LinkedList<URILiteral>(),
				new LinkedList<URILiteral>()); // / workaround weil sonst
												// nullpointerexception
		evaluator.compileQuery(query);
		evaluator.logicalOptimization();
		evaluator.physicalOptimization();

		return evaluator.getResult();
	}

	@Test
	public static void testQ1() throws Exception {
		long start = System.currentTimeMillis();
		String selectQuery = readFile(q1_query_filename);
		QueryResult actual = executeQuery(cloudEvaluator, selectQuery);
		// System.out.println("actual:  " + actual);
		long stop = System.currentTimeMillis();
		result_time[0] = (double) ((stop - start) / (double) 1000);
		System.out.println("Test Q1: "
				+ (double) ((stop - start) / (double) 1000) + " Sekunden - Results: " + actual.getCollection().size());
		result_queryresult[0] = actual.getCollection().size();
	}

	@Test
	public static void testQ2() throws Exception {
		long start = System.currentTimeMillis();
		String selectQuery = readFile(q2_query_filename);
		QueryResult actual = executeQuery(cloudEvaluator, selectQuery);
		// System.out.println("actual:  " + actual);
		long stop = System.currentTimeMillis();
		result_time[1] = (double) ((stop - start) / (double) 1000);
		System.out.println("Test Q2: "
				+ (double) ((stop - start) / (double) 1000) + " Sekunden - Results: " + actual.getCollection().size());
		result_queryresult[1] = actual.getCollection().size();
	}

	@Test
	public static void testQ3a() throws Exception {
		long start = System.currentTimeMillis();
		String selectQuery = readFile(q3a_query_filename);
		QueryResult actual = executeQuery(cloudEvaluator, selectQuery);
		// System.out.println("actual:  " + actual);
		long stop = System.currentTimeMillis();
		result_time[2] = (double) ((stop - start) / (double) 1000);
		System.out.println("Test Q3a: "
				+ (double) ((stop - start) / (double) 1000) + " Sekunden - Results: " + actual.getCollection().size());
		result_queryresult[2] = actual.getCollection().size();
	}

	@Test
	public static void testQ3b() throws Exception {
		long start = System.currentTimeMillis();
		String selectQuery = readFile(q3b_query_filename);
		QueryResult actual = executeQuery(cloudEvaluator, selectQuery);
		// System.out.println("actual:  " + actual);
		long stop = System.currentTimeMillis();
		result_time[3] = (double) ((stop - start) / (double) 1000);
		System.out.println("Test Q3b: "
				+ (double) ((stop - start) / (double) 1000) + " Sekunden - Results: " + actual.getCollection().size());
		result_queryresult[3] = actual.getCollection().size();
	}

	@Test
	public static void testQ3c() throws Exception {
		long start = System.currentTimeMillis();
		String selectQuery = readFile(q3c_query_filename);
		QueryResult actual = executeQuery(cloudEvaluator, selectQuery);
		// System.out.println("actual:  " + actual);
		long stop = System.currentTimeMillis();
		result_time[4] = (double) ((stop - start) / (double) 1000);
		System.out.println("Test Q3c: "
				+ (double) ((stop - start) / (double) 1000) + " Sekunden - Results: " + actual.getCollection().size());
		result_queryresult[4] = actual.getCollection().size();
	}

	@Test
	public static void testQ4() throws Exception {
		long start = System.currentTimeMillis();
		String selectQuery = readFile(q4_query_filename);
		QueryResult actual = executeQuery(cloudEvaluator, selectQuery);
		// System.out.println("actual:  " + actual);
		long stop = System.currentTimeMillis();
		result_time[5] = (double) ((stop - start) / (double) 1000);
		System.out.println("Test Q4 "
				+ (double) ((stop - start) / (double) 1000) + " Sekunden - Results: " + actual.getCollection().size());
		result_queryresult[5] = actual.getCollection().size();
	}

	@Test
	public static void testQ5a() throws Exception {
		long start = System.currentTimeMillis();
		String selectQuery = readFile(q5a_query_filename);
		QueryResult actual = executeQuery(cloudEvaluator, selectQuery);
		// System.out.println("actual:  " + actual);
		long stop = System.currentTimeMillis();
		result_time[6] = (double) ((stop - start) / (double) 1000);
		System.out.println("Test Q5a: "
				+ (double) ((stop - start) / (double) 1000) + " Sekunden - Results: " + actual.getCollection().size());
		result_queryresult[6] = actual.getCollection().size();
	}

	@Test
	public static void testQ5b() throws Exception {
		long start = System.currentTimeMillis();
		String selectQuery = readFile(q5b_query_filename);
		QueryResult actual = executeQuery(cloudEvaluator, selectQuery);
		// System.out.println("actual:  " + actual);
		long stop = System.currentTimeMillis();
		result_time[7] = (double) ((stop - start) / (double) 1000);
		System.out.println("Test Q5b: "
				+ (double) ((stop - start) / (double) 1000) + " Sekunden - Results: " + actual.getCollection().size());
		result_queryresult[7] = actual.getCollection().size();
	}

	@Test
	public static void testQ6() throws Exception {
		long start = System.currentTimeMillis();
		String selectQuery = readFile(q6_query_filename);
		QueryResult actual = executeQuery(cloudEvaluator, selectQuery);
		// System.out.println("actual:  " + actual);
		long stop = System.currentTimeMillis();
		result_time[8] = (double) ((stop - start) / (double) 1000);
		System.out.println("Test Q6: "
				+ (double) ((stop - start) / (double) 1000) + " Sekunden - Results: " + actual.getCollection().size());
		result_queryresult[8] = actual.getCollection().size();
	}

	@Ignore
	@Test
	public static void testQ7() throws Exception {
		long start = System.currentTimeMillis();
		String selectQuery = readFile(q7_query_filename);
		QueryResult actual = executeQuery(cloudEvaluator, selectQuery);
		// System.out.println("actual:  " + actual);
		long stop = System.currentTimeMillis();
		System.out.println("Test Q7: "
				+ (double) ((stop - start) / (double) 1000) + " Sekunden - Results: " + actual.getCollection().size());
		result_time[9] = (double) ((stop - start) / (double) 1000);
		result_queryresult[9] = actual.getCollection().size();
	}

	@Test
	public static void testQ8() throws Exception {
		long start = System.currentTimeMillis();
		String selectQuery = readFile(q8_query_filename);
		QueryResult actual = executeQuery(cloudEvaluator, selectQuery);
		// System.out.println("actual:  " + actual);
		long stop = System.currentTimeMillis();
		result_time[10] = (double) ((stop - start) / (double) 1000);
		System.out.println("Test Q8: "
				+ (double) ((stop - start) / (double) 1000) + " Sekunden - Results: " + actual.getCollection().size());
		result_queryresult[10] = actual.getCollection().size();
	}

	@Ignore
	@Test
	public static void testQ9() throws Exception {
		long start = System.currentTimeMillis();
		String selectQuery = readFile(q9_query_filename);
		QueryResult actual = executeQuery(cloudEvaluator, selectQuery);
		// System.out.println("actual:  " + actual);
		long stop = System.currentTimeMillis();
		result_time[11] = (double) ((stop - start) / (double) 1000);
		System.out.println("Test Q9: "
				+ (double) ((stop - start) / (double) 1000) + " Sekunden - Results: " + actual.getCollection().size());
		result_queryresult[11] = actual.getCollection().size();
	}

	@Test
	public static void testQ10() throws Exception {
		long start = System.currentTimeMillis();
		String selectQuery = readFile(q10_query_filename);
		QueryResult actual = executeQuery(cloudEvaluator, selectQuery);
		// System.out.println("actual:  " + actual);
		long stop = System.currentTimeMillis();
		result_time[12] = (double) ((stop - start) / (double) 1000);
		System.out.println("Test Q10: "
				+ (double) ((stop - start) / (double) 1000) + " Sekunden - Results: " + actual.getCollection().size());
		result_queryresult[12] = actual.getCollection().size();
	}

	@Test
	public static void testQ11() throws Exception {
		long start = System.currentTimeMillis();
		String selectQuery = readFile(q11_query_filename);
		QueryResult actual = executeQuery(cloudEvaluator, selectQuery);
		// System.out.println("actual:  " + actual);
		long stop = System.currentTimeMillis();
		result_time[13] = (double) ((stop - start) / (double) 1000);
		System.out.println("Test Q11: "
				+ (double) ((stop - start) / (double) 1000) + " Sekunden - Results: " + actual.getCollection().size());
		result_queryresult[13] = actual.getCollection().size();
	}

	@Test
	public static void testQ12a() throws Exception {
		long start = System.currentTimeMillis();
		String selectQuery = readFile(q12a_query_filename);
		QueryResult actual = executeQuery(cloudEvaluator, selectQuery);
		// System.out.println("actual:  " + actual);
		long stop = System.currentTimeMillis();
		result_time[14] = (double) ((stop - start) / (double) 1000);
		System.out.println("Test Q12a: "
				+ (double) ((stop - start) / (double) 1000) + " Sekunden - Results: " + actual.getCollection().size());
		result_queryresult[14] = actual.getCollection().size();
	}

	@Test
	public static void testQ12b() throws Exception {
		long start = System.currentTimeMillis();
		String selectQuery = readFile(q12b_query_filename);
		QueryResult actual = executeQuery(cloudEvaluator, selectQuery);
		// System.out.println("actual:  " + actual);
		long stop = System.currentTimeMillis();
		result_time[15] = (double) ((stop - start) / (double) 1000);
		System.out.println("Test Q12b: "
				+ (double) ((stop - start) / (double) 1000) + " Sekunden - Results: " + actual.getCollection().size());
		result_queryresult[15] = actual.getCollection().size();
	}

	@Test
	public static void testQ12c() throws Exception {
		long start = System.currentTimeMillis();
		String selectQuery = readFile(q12c_query_filename);
		QueryResult actual = executeQuery(cloudEvaluator, selectQuery);
		// System.out.println("actual:  " + actual);
		long stop = System.currentTimeMillis();
		result_time[16] = (double) ((stop - start) / (double) 1000);
		System.out.println("Test Q12c: "
				+ (double) ((stop - start) / (double) 1000) + " Sekunden - Results: " + actual.getCollection().size());
		result_queryresult[16] = actual.getCollection().size();
	}
}
