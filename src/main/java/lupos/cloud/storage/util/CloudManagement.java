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
package lupos.cloud.storage.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;

import lupos.cloud.hbase.HBaseConnection;
import lupos.cloud.hbase.HBaseDistributionStrategy;
import lupos.cloud.hbase.HBaseTriple;
import lupos.cloud.pig.PigQuery;
import lupos.cloud.pig.SinglePigQuery;
import lupos.datastructures.bindings.Bindings;
import lupos.datastructures.items.Triple;
import lupos.datastructures.items.Variable;
import lupos.datastructures.items.literal.LiteralFactory;
import lupos.datastructures.queryresult.QueryResult;
import lupos.misc.util.ImmutableIterator;

/**
 * Diese Klasse ist für die Kommunikation mit der Cloud zuständig (sowohl HBase
 * als auch MapReduce/Pig).
 */
public class CloudManagement {

	/** The count triple. */
	public static int countTriple = 0;

	/** The pig server. */
	static PigServer pigServer = null;

	/** The pig query result. */
	Iterator<Tuple> pigQueryResult = null;

	/** The cur variable list. */
	ArrayList<String> curVariableList = null;

	boolean PRINT_PIGLATIN_PROGRAMM = true;

	/**
	 * Instantiates a new cloud management.
	 */
	public CloudManagement() {

		try {
			HBaseConnection.init();
			pigServer = new PigServer(ExecType.MAPREDUCE);
			for (String tablename : HBaseDistributionStrategy
					.getTableInstance().getTableNames()) {
				HBaseConnection.createTable(tablename,
						HBaseDistributionStrategy.getTableInstance()
								.getColumnFamilyName());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Submit h base triple to database.
	 * 
	 * @param triple
	 *            the triple
	 */
	public void submitHBaseTripleToDatabase(final Collection<HBaseTriple> triple) {
		for (HBaseTriple item : triple) {
			if (countTriple % 10000 == 0) {
				if (countTriple != 0) {
					System.out
							.println(countTriple + " HBaseTripel importiert!");
				}
			}
			try {
				HBaseConnection.addRow(item.getTablename(), item.getRow_key(),
						item.getColumnFamily(), item.getColumn(),
						item.getValue());
				countTriple++;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Delete h base triple from database.
	 * 
	 * @param triple
	 *            the triple
	 */
	public void deleteHBaseTripleFromDatabase(
			final Collection<HBaseTriple> triple) {
		try {
			for (HBaseTriple item : triple) {
				HBaseConnection.deleteRow(item.getTablename(),
						item.getColumnFamily(), item.getRow_key(),
						item.getColumn());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Submit pig query.
	 * 
	 * @param query
	 *            the query
	 * @return the query result
	 */
	public QueryResult submitPigQuery(final PigQuery query) {
//		if (!"a".equals("b"))
//		return null; // testing purpose
		QueryResult result = null;
		long start = System.currentTimeMillis();
		try {
			if (PRINT_PIGLATIN_PROGRAMM) {
				System.out.println("Generated PigLatin Program:");
				System.out.println(query.getPigLatin());
				System.out.println();
			}
			
//			if (!"a".equals("b"))
//			return null; // testing purpose
			System.out.println("PigLatin Programm wird ausgeführt...");
			pigServer.registerQuery(query.getPigLatin());
			curVariableList = query.getVariableList();
			pigQueryResult = pigServer.openIterator("X");
			result = new QueryResult();
			result = QueryResult
					.createInstance(new ImmutableIterator<Bindings>() {
						@Override
						public boolean hasNext() {
							return pigQueryResult.hasNext();
						}

						@Override
						public Bindings next() {
							if (this.hasNext()) {
								try {
									final Bindings result = Bindings
											.createNewInstance();
									Tuple tuple = pigQueryResult.next();
									int i = 0;
									for (String var : curVariableList) {
										Object curTupleObject = tuple.get(i);
										// unbounded Variables
										if (curTupleObject == null) {
											result.add(new Variable(var), null);
											continue;
										}
										
										String curTupel = curTupleObject.toString();
										if (curTupel.toString().startsWith("<")) {
											result.add(
													new Variable(var),
													LiteralFactory
															.createURILiteral(tuple
																	.get(i)
																	.toString()));
										} else if (curTupel.startsWith("\"")) {
											String content = curTupel.substring(
													curTupel.indexOf("\""),
													curTupel.lastIndexOf("\"") + 1);
											int startIndex = curTupel
													.indexOf("<");
											int stopIndex = curTupel
													.indexOf(">") + 1;
											if (startIndex != -1
													&& stopIndex != -1) {
												String type = curTupel
														.substring(startIndex,
																stopIndex);
												result.add(
														new Variable(var),
														LiteralFactory
																.createTypedLiteral(
																		content,
																		type));
											} else {
												result.add(
														new Variable(var),
														LiteralFactory
																.createLiteral(content));
											}
										} else if (curTupel.startsWith("_:")) {
											result.add(
													new Variable(var),
													LiteralFactory
															.createAnonymousLiteral(curTupel));
										} else if (curTupel.startsWith("_:")) {
											result.add(
													new Variable(var),
													LiteralFactory
															.createAnonymousLiteral(curTupel));
										} else {
											result.add(
													new Variable(var),
													LiteralFactory
															.createLiteral(tuple
																	.get(i)
																	.toString()));
										}
										i++;
									}
									return result;
								} catch (Exception e) {
									e.printStackTrace();
									return null;
								}
							} else {
								return null;
							}
						}
					});
		} catch (IOException e) {
			e.printStackTrace();
		}
		long stop = System.currentTimeMillis();
		System.out.println("PigLatin Programm erfolgreich in "
				+ ((stop - start) / 1000) + "s ausgeführt!");
		return result;
	}
}
