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

import java.util.ArrayList;
import java.util.Collection;

import lupos.cloud.hbase.HBaseTableStrategy;
import lupos.cloud.hbase.HBaseTriple;
import lupos.datastructures.items.Triple;
import lupos.datastructures.items.Variable;
import lupos.datastructures.items.literal.AnonymousLiteral;
import lupos.datastructures.items.literal.Literal;
import lupos.engine.operators.tripleoperator.TriplePattern;

/**
 * Helper class to build SPARUL queries for manipulating and querying the
 * distributed endpoints
 */
public class CloudQueryBuilder {
	static int patternCounter = 0;
	static ArrayList<Integer> patternToJoin = new ArrayList<Integer>();

	// /**
	// * Builds an insert-data-query, which inserts all given triples
	// * @param toBeAdded the triples to be inserted
	// * @return a SPARUL query for inserting the given triples
	// */
	// public static String buildInsertQuery(final Collection<Triple>
	// toBeAdded){
	// // insert all triples of toBeAdded by using one INSERT DATA query!
	// StringBuilder sb = new StringBuilder();
	// sb.append("INSERT DATA { ");
	//
	// for(Triple triple: toBeAdded) {
	// sb.append(CloudQueryBuilder.toN3StringReplacingBlankNodesWithIRIs(triple));
	// sb.append(" ");
	// }
	// sb.append(" }");
	// return sb.toString();
	// }

	public static Collection<HBaseTriple> buildInputHBaseTriple(
			final Collection<Triple> toBeAdded) {
		// !!! EDIT !!! ... statt dem SPARQL Query wird nun eine Liste von HBase
		// Tripeln erzeugt
		ArrayList<HBaseTriple> hbaseTripleList = new ArrayList<HBaseTriple>();

		for (Triple triple : toBeAdded) {
			for (HBaseTriple ht : HBaseTableStrategy
					.generateSixIndecesTriple(triple))
				hbaseTripleList.add(ht);
		}
		return hbaseTripleList;
	}

	// /**
	// * Builds a query to check if a triple is contained in the distributed
	// * storage
	// *
	// * @param triple
	// * the triple to be checked for containment
	// * @return the query for checking the containment of the given triple
	// */
	// public static String buildQuery(Triple triple) {
	// return "SELECT * WHERE { "
	// + CloudQueryBuilder
	// .toN3StringReplacingBlankNodesWithIRIs(triple) + " }";
	// }

	public static String buildQuery(Triple triple) {
		// TODO: Pig Query erzeugen zum suchen von Triple
		return "PIG Query";
	}

	// /**
	// * Builds a query to evaluate the given triple pattern
	// *
	// * @param triplePattern
	// * the triple pattern to be evaluated
	// * @return the query for evaluating the given triple pattern
	// */
	// public static String buildQuery(TriplePattern triplePattern) {
	// return "SELECT * WHERE { " + triplePattern.toN3String() + " }";
	// }

	public static String buildQuery(TriplePattern triplePattern) {
		StringBuilder result = new StringBuilder();
		String hBaseTable = getHBaseTable(triplePattern);

		String literals = "";
		String variables = "";

		result.append(hBaseTable
				+ "_DATA = "
				+ "load 'hbase://"
				+ hBaseTable
				+ "'"
				+ "using org.apache.pig.backend.hadoop.hbase.HBaseStorage('VALUE', '-loadKey true');");
		result.append("PATTERN" + patternCounter + " = FILTER "
				+ hBaseTable + " data BY $0 == '" + literals
				+ "';");
		patternCounter++;
		return result.toString();
	}

	private static String getHBaseTable(TriplePattern triplePattern) {
		int subject = triplePattern.getSubject().getClass() == Variable.class ? 1
				: 0;
		int predicate = triplePattern.getPredicate().getClass() == Variable.class ? 10
				: 0;
		int object = triplePattern.getObject().getClass() == Variable.class ? 100
				: 0;

		String result = null;
		switch (subject + predicate + object) {
		case 110:
			result = "S_PO";
			break;
		case 101:
			result = "P_SO";
			break;
		case 11:
			result = "O_SP";
			break;
		case 100:
			result = "PS_O";
			break;
		case 10:
			result = "SO_P";
			break;
		case 1:
			result = "PO_S";
			break;
		default:
			// TODO: SPO und ??? fehlen
			break;
		}
		return result;
	}

	/**
	 * Blank nodes cannot occur in SPARUL insertions and deletions. This method
	 * therefore replaces blank nodes in triples...
	 * 
	 * @param triple
	 *            the triple
	 * @return A N3 string representation of the triple, where blank nodes have
	 *         been replaced with iris
	 */
	public static String toN3StringReplacingBlankNodesWithIRIs(Triple triple) {
		StringBuilder sb = new StringBuilder();

		for (Literal literal : triple) {
			sb.append(CloudQueryBuilder
					.toN3StringReplacingBlankNodesWithIRIs(literal));
			sb.append(" ");
		}
		sb.append(".");

		return sb.toString();
	}

	/**
	 * Blank nodes cannot occur in SPARUL insertions and deletions. This method
	 * therefore returns an iri for a given blank node or just the string
	 * representation otherwise
	 * 
	 * @param literal
	 *            the literal
	 * @return string representation of the literal (for blank nodes an iri is
	 *         returned)
	 */
	public static String toN3StringReplacingBlankNodesWithIRIs(
			final Literal literal) {
		if (literal instanceof AnonymousLiteral) {
			return "<http://www.ifis.uni-luebeck.de/blank_node/"
					+ ((AnonymousLiteral) literal).getBlankNodeLabel() + ">";
		} else {
			return literal.toString();
		}
	}
}
