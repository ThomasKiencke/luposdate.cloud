package lupos.cloud.pig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import lupos.cloud.pig.operator.PigFilterOperator;
import lupos.cloud.testing.CloudBitvector;
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

	HashSet<String> optionalJoinElements = new HashSet<String>();

	/** The triple pattern. */
	TriplePattern triplePattern;

	ArrayList<PigFilterOperator> appliedFilters = new ArrayList<PigFilterOperator>();

	private String tablename;

	private HashMap<String, HashSet<CloudBitvector>> bitVectors = new HashMap<String, HashSet<CloudBitvector>>();

	public JoinInformation() {
		this.name = "INTERMEDIATE_BAG_" + JoinInformation.idCounter;
		this.setPatternId(idCounter);
		idCounter++;
	}

	/**
	 * Instantiates a new join information.
	 * 
	 * @param triplePattern
	 *            the triple pattern
	 * @param name
	 *            the name
	 */
	public JoinInformation(TriplePattern triplePattern, String tablename,
			String name) {
		super();
		this.triplePattern = triplePattern;
		this.patternId = idCounter;
		this.name = name + this.patternId;
		idCounter++;
		this.tablename = tablename;
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
		this.name = name;
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

	public void addJoinElements(String elem) {
		this.joinElements.add(elem);
	}

	public void addOptionalElements(String elem) {
		this.optionalJoinElements.add(elem);
	}

	public HashSet<String> getOptionalJoinElements() {
		return optionalJoinElements;
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

//	public String getFirstLiteral() {
//		boolean first = true;
//		for (Item item : triplePattern.getItems()) {
//			if (!item.isVariable()) {
//				if (first) {
//					return item.toString();
//				}
//				first = false;
//			}
//		}
//		return null;
//	}
//
//	public String getSecondLiteral() {
//		boolean first = true;
//		for (Item item : triplePattern.getItems()) {
//			if (!item.isVariable()) {
//				if (!first) {
//					return item.toString();
//				}
//				first = false;
//			}
//		}
//		return null;
//	}

	public boolean isVariableOptional(String var) {
		return optionalJoinElements.contains(var);
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

	public String getTablename() {
		return tablename;
	}

	public void addAppliedFilters(PigFilterOperator appliedFilter) {
		this.appliedFilters.add(appliedFilter);
	}

	public void addAppliedFilters(ArrayList<PigFilterOperator> appliedFilters) {
		for (PigFilterOperator filter : appliedFilters) {
			this.appliedFilters.add(filter);
		}
	}

	public ArrayList<PigFilterOperator> getAppliedFilters() {
		return appliedFilters;
	}

	public boolean filterApplied(PigFilterOperator appliedFilter) {
		return this.appliedFilters.contains(appliedFilter);
	}

	public static ArrayList<PigFilterOperator> mergeAppliedFilters(
			ArrayList<JoinInformation> joins) {
		ArrayList<PigFilterOperator> result = new ArrayList<PigFilterOperator>();
		for (JoinInformation j1 : joins) {
			for (PigFilterOperator filter1 : j1.getAppliedFilters()) {
				boolean filterInEveryJoin = true;
				for (JoinInformation j2 : joins) {
					if (!j2.getAppliedFilters().contains(filter1)) {
						filterInEveryJoin = false;
					}
				}
				if (filterInEveryJoin) {
					result.add(filter1);
				}

			}
		}
		return result;
	}

	public void mergeOptionalVariables(ArrayList<JoinInformation> inputBags) {
		for (JoinInformation bag : inputBags) {
			this.mergeOptionalVariables(bag);
		}
	}

	public void mergeOptionalVariables(JoinInformation bag) {
		for (String var : bag.getOptionalJoinElements()) {
			this.optionalJoinElements.add(var);
		}
	}

	public void addBitvector(String var, CloudBitvector vector) {
		HashSet<CloudBitvector> list = this.bitVectors.get(var);
		if (list == null) {
			list = new HashSet<CloudBitvector>();
			list.add(vector);
			this.bitVectors.put(var, list);
		} else {
			list.add(vector);
		}
	}

	public HashMap<String, HashSet<CloudBitvector>> getBitVectors() {
		return bitVectors;
	}

	public HashSet<CloudBitvector> getBitVector(String var) {
		return bitVectors.get(var);
	}
	
	public void addBitVectors(
			HashMap<String, HashSet<CloudBitvector>> bitVectors) {
		for (String key : bitVectors.keySet()) {
			HashSet<CloudBitvector> list = this.bitVectors.get(key);
			if (list == null) {
				list = new HashSet<CloudBitvector>();
				for (CloudBitvector v : bitVectors.get(key)) {
					list.add(v);
				}
				this.bitVectors.put(key, list);

			} else {
				for (CloudBitvector v : bitVectors.get(key)) {
					list.add(v);
				}
				this.bitVectors.put(key, list);
			}
		}
	}

	public void addBitvector(String var, HashSet<CloudBitvector> bitVectors) {
		if (bitVectors == null) {
			return;
		}
		
		HashSet<CloudBitvector> list = this.bitVectors.get(var);
		if (list == null) {
			list = new HashSet<CloudBitvector>();
			for (CloudBitvector v : bitVectors) {
				list.add(v);
			}
			this.bitVectors.put(var, list);

		} else {
			for (CloudBitvector v : bitVectors) {
				list.add(v);
			}
			this.bitVectors.put(var, list);
		}
	}

	public void mergeBitVecor(String var, HashSet<CloudBitvector> bitVectors) {
		if (bitVectors == null) {
			return;
		}
		
		HashSet<CloudBitvector> list = this.bitVectors.get(var);
		if (list == null) {
			list = new HashSet<CloudBitvector>();
			for (CloudBitvector v : bitVectors) {
				v.setInc();
				list.add(v);
			}
			this.bitVectors.put(var, list);

		} else {
			for (CloudBitvector v : bitVectors) {
				v.setInc();
				list.add(v);
			}
			this.bitVectors.put(var, list);
		}
		
	}

}
