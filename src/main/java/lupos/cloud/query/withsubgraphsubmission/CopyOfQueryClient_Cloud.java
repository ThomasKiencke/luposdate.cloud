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
package lupos.cloud.query.withsubgraphsubmission;

//import lupos.cloud.gui.Start_Demo_Applet_DE;
//import lupos.cloud.storage.HistogramExecutor;
import lupos.cloud.storage.Storage_DE;
import lupos.datastructures.bindings.Bindings;
import lupos.datastructures.bindings.BindingsMap;
import lupos.datastructures.items.literal.LiteralFactory;
import lupos.distributed.query.QueryClient;

/**
 * This class is the query evaluator for querying distributed SPARQL endpoints.
 * All registered endpoints are asked for the evaluation of the triple patterns within a SPARQL query.
 * It is assumed that the data is not distributed in an intelligent way and that any registered endpoint
 * can have data for any triple pattern.
 * Also non-luposdate SPARQL endpoints are supported.
 * It uses the super and helper classes of the distributed module for a first and simple example of a distributed scenario.
 */
public class CopyOfQueryClient_Cloud extends QueryClient {

	public CopyOfQueryClient_Cloud() throws Exception {
		super(new Storage_DE());
//		this.askForHistogramRequests();
	}

	public CopyOfQueryClient_Cloud(final String[] args) throws Exception {
		super(new Storage_DE(), args);
//		this.askForHistogramRequests();
	}

//	private void askForHistogramRequests(){
//		if(Start_Demo_Applet_DE.askForHistogramRequests()){
//			this.histogramExecutor = new HistogramExecutor(((Storage_DE)this.storage).getEndpointManagement());
//			this.initOptimization();
//		}
//	}


	@Override
	public void init() throws Exception {
		// just for avoiding problems in distributed scenarios
		Bindings.instanceClass = BindingsMap.class;
		LiteralFactory.setType(LiteralFactory.MapType.NOCODEMAP);
		super.init();
	}
}
