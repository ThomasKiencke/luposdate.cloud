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

//import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
//import java.io.IOException;
//import java.io.InputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
//import java.util.concurrent.TimeUnit;

//import org.apache.hadoop.hbase.thrift.generated.Hbase;



import lupos.cloud.hbase.HBaseConnection;
import lupos.cloud.hbase.HBaseTableStrategy;
import lupos.cloud.hbase.HBaseTriple;
import lupos.cloud.pig.PigQuery;
import lupos.cloud.pig.udfs.MapToBag;
import lupos.cloud.pig.udfs.PigLoadUDF;
import lupos.cloud.testing.JarGetter;
import lupos.datastructures.bindings.Bindings;
import lupos.datastructures.items.Variable;
import lupos.datastructures.items.literal.LiteralFactory;
import lupos.datastructures.queryresult.QueryResult;
//import lupos.distributed.storage.distributionstrategy.tripleproperties.KeyContainer;
//import lupos.endpoint.client.Client;
//import lupos.engine.operators.multiinput.join.parallel.ResultCollector;
import lupos.misc.util.ImmutableIterator;

//import lupos.misc.Tuple;

public class CloudManagement {

	/**
	 * contains the registered SPARQL endpoints...
	 */
	protected String[] urlsOfEndpoints;

	public static final boolean enableHbase = true;
	public static int countTriple = 0;
	static PigServer pigServer = null;
	Iterator<Tuple> pigQueryResult = null;
	ArrayList<String> curVariableList = null;

	/**
	 * Reads in the registered SPARQL endpoints from the configuration file
	 * /endpoints.txt. Each line of this file must contain the URL of a SPARQL
	 * endpoint.
	 */
	public CloudManagement() {

		try {
			HBaseConnection.init();
			// pigServer = new PigServer(ExecType.MAPREDUCE);
			// Properties props = new Properties();
			// props.setProperty("fs.default.name", "hdfs://localhost:8020");
			// props.setProperty("mapred.job.tracker", "localhost:8021");
			// props.setProperty("hbase.zookeeper.quorum", "localhost");
			// props.setProperty("hbase.zookeeper.property.clientPort", "2181");
			// pigServer = new PigServer(ExecType.MAPREDUCE, props);
			pigServer = new PigServer(ExecType.MAPREDUCE);
			pigServer.registerJar(JarGetter.getJar(PigLoadUDF.class));
			pigServer.registerJar(JarGetter.getJar(MapToBag.class));
			pigServer.registerJar(JarGetter.getJar(com.google.protobuf.Message.class));
			for (String tablename : HBaseTableStrategy.getTableInstance()
					.getTableNames()) {
				HBaseConnection.createTable(tablename, HBaseTableStrategy
						.getTableInstance().getColumnFamilyName());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void submitHBaseTripleToDatabase(final Collection<HBaseTriple> triple) {
		// TODO: Connection To Hbase + Insert
		// seinding triple to hbase as row_key, family ...
		for (HBaseTriple item : triple) {
			// TODO: HBase Connection herstellen + Tripel in die DB laden
			if (countTriple % 500 == 0) {
				System.out.println(countTriple + " HBaseTripel importiert!");
			}
			try {
				HBaseConnection.addRow(item.getTablename(), item.getRow_key(),
						item.getColumnFamily(), item.getColumn(),
						item.getValue());
				countTriple++;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void deleteHBaseTripleFromDatabase(
			final Collection<HBaseTriple> triple) {
		// TODO: Connection to HBase + Delete
		// seinding triple to hbase as row_key, family ...
		// for (HBaseTriple item : triple) {
		// try {
		// HBaseConnection.addRow(item.getTablename(), item.getRow_key(),
		// item.getColumn());
		// } catch (IOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		// }
	}

	public QueryResult submitPigQuery(final PigQuery query) {
		QueryResult result = null;
		long start = System.currentTimeMillis();
		try {
			System.out.println("Generated PigLatin Program:");
			System.out.println(query.getPigLatin());
			System.out.println();
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
										String curTupel = tuple.get(i)
												.toString();
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
											String type = curTupel.substring(
													curTupel.indexOf("<"),
													curTupel.indexOf(">") + 1);
											result.add(
													new Variable(var),
													LiteralFactory
															.createTypedLiteral(
																	content,
																	type));
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
									// TODO Auto-generated catch block
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

	public QueryResult submitSubgraphQuery(String subgraphSerializedAsJSON) {
		// TODO Auto-generated method stub
		return null;
	}
}
