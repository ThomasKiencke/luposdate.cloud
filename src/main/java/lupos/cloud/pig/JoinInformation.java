package lupos.cloud.pig;

import java.util.ArrayList;

import lupos.datastructures.items.Item;
import lupos.engine.operators.tripleoperator.TriplePattern;

/**
 * In dieser Klasse werden Informationen über die einzelnen Tripel-Muster bzw.
 * der JOIN's mehrere Tripelpattern.
 */
public class JoinInformation {

	/** The id counter. */
	public static Integer idCounter = 0;

	/** The pattern id. */
	Integer patternId;

	/** The name. */
	String name;

	/** The join elements. */
	ArrayList<String> joinElements = new ArrayList<String>();

	/** The triple pattern. */
	TriplePattern triplePattern;

	/**
	 * Instantiates a new join information.
	 * 
	 * @param triplePattern
	 *            the triple pattern
	 * @param name
	 *            the name
	 */
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

	/**
	 * Instantiates a new join information.
	 * 
	 * @param name
	 *            the name
	 */
	public JoinInformation(String name) {
	}

	/**
	 * Gets the pattern id.
	 * 
	 * @return the pattern id
	 */
	public Integer getPatternId() {
		return patternId;
	}

	/**
	 * Sets the pattern id.
	 * 
	 * @param patternId
	 *            the new pattern id
	 */
	public void setPatternId(Integer patternId) {
		this.patternId = patternId;
	}

	/**
	 * Gets the name.
	 * 
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * Sets the name.
	 * 
	 * @param name
	 *            the new name
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * Gets the join elements.
	 * 
	 * @return the join elements
	 */
	public ArrayList<String> getJoinElements() {
		return joinElements;
	}

	/**
	 * Sets the join elements.
	 * 
	 * @param joinElements
	 *            the new join elements
	 */
	public void setJoinElements(ArrayList<String> joinElements) {
		this.joinElements = joinElements;
	}

	/**
	 * Gets the literals.
	 * 
	 * @return the literals
	 */
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

	/**
	 * Gets the variables.
	 * 
	 * @return the variables
	 */
	public ArrayList<String> getVariables() {
		ArrayList<String> result = new ArrayList<String>();
		if (triplePattern != null) {
			for (Item item : triplePattern.getItems()) {
				if (item.isVariable()) {
					result.add(item.toString());
				}
			}
		}
		return result;
	}

	/**
	 * Gets the item pos.
	 * 
	 * @param itemID
	 *            the item id
	 * @return the item pos
	 */
	public Integer getItemPos(String itemID) {
		return this.joinElements.indexOf(itemID);
	}

	/**
	 * All elements are variables.
	 * 
	 * @return true, if successful
	 */
	public boolean allElementsAreVariables() {
		for (Item item : triplePattern.getItems()) {
			if (!item.isVariable()) {
				return false;
			}
		}
		return true;
	}

	/**
	 * All elements are literals.
	 * 
	 * @return true, if successful
	 */
	public boolean allElementsAreLiterals() {
		for (Item item : triplePattern.getItems()) {
			if (item.isVariable()) {
				return false;
			}
		}
		return true;
	}
}
