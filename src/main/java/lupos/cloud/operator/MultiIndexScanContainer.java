package lupos.cloud.operator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.TreeMap;

import lupos.datastructures.queryresult.QueryResult;
import lupos.engine.operators.BasicOperator;
import lupos.engine.operators.RootChild;
import lupos.engine.operators.index.Dataset;
import lupos.engine.operators.multiinput.MultiInputOperator;
import lupos.engine.operators.singleinput.Projection;

public class MultiIndexScanContainer extends BasicOperator {
	public static final int UNION = 0;
	public static final Integer JOIN = 1;
	private static int idCounter = 0;
	private ArrayList<BasicOperator> ops = new ArrayList<BasicOperator>();
	private static final long serialVersionUID = -5612770902234058839L;

	TreeMap<Integer, HashSet<BasicOperator>> multiIndexScanList = new TreeMap<Integer, HashSet<BasicOperator>>();
	TreeMap<Integer, MultiInputOperator> mappingTree = new TreeMap<Integer, MultiInputOperator>();

	public void addSubContainer(MultiInputOperator type,
			HashSet<BasicOperator> ops) {
		multiIndexScanList.put(idCounter, ops);
		mappingTree.put(idCounter, type);
		idCounter++;
	}

	public TreeMap<Integer, HashSet<BasicOperator>> getContainerList() {
		return multiIndexScanList;
	}

	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
		result.append("--- MultiIndexScanContainer ---\n");
		// for (HashSet<BasicOperator> curNodes : multiIndexScanList.values()) {
		// for (BasicOperator curNode : curNodes) {
		// if (curNode instanceof MultiIndexScanContainer) {
		// result.append(((MultiIndexScanContainer) curNode).toString());
		// } else if (curNode instanceof IndexScanContainer) {
		// result.append(((IndexScanContainer) curNode).toString());
		// } else {
		// result.append("\n" + curNode.getClass().getSimpleName());
		// }
		// }
		//
		// }

		return result.toString();
	}

	public TreeMap<Integer, MultiInputOperator> getMappingTree() {
		return mappingTree;
	}

	public void addOperatorToAllChilds(BasicOperator op) {
		for (HashSet<BasicOperator> curList : multiIndexScanList.values()) {
			for (BasicOperator node : curList) {
				if (node instanceof IndexScanContainer) {
					((IndexScanContainer) node).addOperator(op);
				} else {
					((MultiIndexScanContainer) node).addOperatorToAllChilds(op);
				}
			}
		}
	}

	public void addOperator(BasicOperator op) {
		this.ops.add(op);
	}

	public ArrayList<BasicOperator> getOperators() {
		return ops;
	}

}
