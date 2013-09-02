package lupos.cloud.operator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
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

	TreeMap<Integer, LinkedList<BasicOperator>> multiIndexScanList = new TreeMap<Integer, LinkedList<BasicOperator>>();
	TreeMap<Integer, MultiInputOperator> mappingTree = new TreeMap<Integer, MultiInputOperator>();

	public void addSubContainer(MultiInputOperator type,
			LinkedList<BasicOperator> ops) {
		multiIndexScanList.put(idCounter, ops);
		mappingTree.put(idCounter, type);
		idCounter++;
	}

	public TreeMap<Integer, LinkedList<BasicOperator>> getContainerList() {
		return multiIndexScanList;
	}

	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
		result.append("--- MultiIndexScanContainer ---\n");
		for (BasicOperator op : ops) {
			if (op instanceof Projection) {
				result.append(((Projection) op).toString() + "\n");
			} else {
				result.append(op.getClass().getSimpleName() + "\n");
			}
		}

		return result.toString();
	}

	public TreeMap<Integer, MultiInputOperator> getMappingTree() {
		return mappingTree;
	}

	public void addOperatorToAllChilds(BasicOperator op) {
		for (LinkedList<BasicOperator> curList : multiIndexScanList.values()) {
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
