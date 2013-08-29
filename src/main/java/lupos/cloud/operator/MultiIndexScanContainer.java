package lupos.cloud.operator;

import java.util.HashSet;
import java.util.TreeMap;

import lupos.engine.operators.BasicOperator;

public class MultiIndexScanContainer extends BasicOperator {
	public static final int UNION = 0;
	public static final Integer JOIN = 1;
	private static int idCounter = 0;
//	private int id;

//	public MultiIndexScanContainer() {
//		this.id = idCounter;
//		idCounter++;
//	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -5612770902234058839L;

	TreeMap<Integer, HashSet<BasicOperator>> multiIndexScanList = new TreeMap<Integer, HashSet<BasicOperator>>();
	TreeMap<Integer, Integer> mappingTree = new TreeMap<Integer, Integer>();

	public void addOperator(Integer type, HashSet<BasicOperator> ops) {
		multiIndexScanList.put(idCounter, ops);
		mappingTree.put(idCounter, type);
		idCounter++;
	}

	public TreeMap<Integer, HashSet<BasicOperator>> getOperatorList() {
		return multiIndexScanList;
	}

	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
		result.append("--- Cloud Union ---\n");
		// for (BasicOperator curNode : multiIndexScanList.get) {
		// result.append("\n" + curNode.getClass().getSimpleName());
		//
		// }

		return result.toString();
	}

	// public ArrayList<BasicIndexScan> getIndexScanList() {
	// return indexScanList;
	// }

	public TreeMap<Integer, Integer> getMappingTree() {
		return mappingTree;
	}

//	public int getId() {
//		return id;
//	}
}
