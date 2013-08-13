package lupos.cloud.pig;

import java.util.ArrayList;
import java.util.HashMap;

import com.hp.hpl.jena.graph.query.Expression.Variable;

import lupos.datastructures.items.Item;
import lupos.engine.operators.tripleoperator.TriplePattern;

public class JoinInformation {
	public static Integer idCounter = 0;
	Integer patternId;
	String name;
	ArrayList<String> joinElements = new ArrayList<String>();
	TriplePattern triplePattern;

	public JoinInformation(TriplePattern triplePattern, String name) {
		super();
		this.triplePattern = triplePattern;
		this.patternId = idCounter;
		idCounter++;
		this.name = name;
		for (Item item : triplePattern.getItems()) {
			if (item.isVariable()) {
				joinElements.add(item.toString());
			}
		}
	}

	public JoinInformation(String name) {
	}

	public Integer getPatternId() {
		return patternId;
	}

	public void setPatternId(Integer patternId) {
		this.patternId = patternId;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public ArrayList<String> getJoinElements() {
		return joinElements;
	}

	public void setJoinElements(ArrayList<String> joinElements) {
		this.joinElements = joinElements;
	}

	public String getLiterals() {
		StringBuilder result = new StringBuilder();
		boolean first = true;
		for (Item item : triplePattern.getItems()) {
			if (!item.isVariable()) {
				result.append(first ? item.toString() : "," + item.toString());
				first = false;
			}
		}
		return result.toString();
	}

	public ArrayList<String> getVariables() {
		ArrayList<String> result = new ArrayList<String>();
		for (Item item : triplePattern.getItems()) {
			if (item.isVariable()) {
				result.add(item.toString());
			}
		}
		return result;
	}

	public Integer getItemPos(String itemID) {
		return this.joinElements.indexOf(itemID);
	}

	public boolean allElementsAreVariables() {
		for (Item item : triplePattern.getItems()) {
			if (!item.isVariable()) {
				return false;
			}
		}
		return true;
	}
}
