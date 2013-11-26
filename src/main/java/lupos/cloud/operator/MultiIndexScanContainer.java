package lupos.cloud.operator;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.TreeMap;

import lupos.engine.operators.BasicOperator;
import lupos.engine.operators.multiinput.MultiInputOperator;
import lupos.engine.operators.singleinput.Projection;

/**
 * Enthält EINEN MultiIndexScan-Operator und die Folgeoprationen.
 */
public class MultiIndexScanContainer extends BasicOperator {

	/** Id counter. */
	private static int idCounter = 0;

	/** Folgeoperation. */
	private ArrayList<BasicOperator> ops = new ArrayList<BasicOperator>();

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = -5612770902234058839L;

	/** The multi index scan list. */
	TreeMap<Integer, LinkedList<BasicOperator>> multiIndexScanList = new TreeMap<Integer, LinkedList<BasicOperator>>();

	/** Mapping tree. */
	TreeMap<Integer, MultiInputOperator> mappingTree = new TreeMap<Integer, MultiInputOperator>();

	/** True, wenn eine Operation nicht unterstützt wird. */
	private boolean oneOperatorWasNotSupported;

	/**
	 * Fügt einen Container hinzu.
	 * 
	 * @param type
	 *            the type
	 * @param ops
	 *            the ops
	 */
	public void addSubContainer(MultiInputOperator type,
			LinkedList<BasicOperator> ops) {
		multiIndexScanList.put(idCounter, ops);
		mappingTree.put(idCounter, type);
		idCounter++;
	}

	/**
	 * Gets the container list.
	 * 
	 * @return the container list
	 */
	public TreeMap<Integer, LinkedList<BasicOperator>> getContainerList() {
		return multiIndexScanList;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see lupos.engine.operators.BasicOperator#toString()
	 */
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

	/**
	 * Gets the mapping tree.
	 * 
	 * @return the mapping tree
	 */
	public TreeMap<Integer, MultiInputOperator> getMappingTree() {
		return mappingTree;
	}

	/**
	 * Adds the operator to all childs.
	 * 
	 * @param op
	 *            the op
	 */
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

	/**
	 * Adds the operator.
	 * 
	 * @param op
	 *            the op
	 */
	public void addOperator(BasicOperator op) {
		this.ops.add(op);
	}

	/**
	 * Gets the operators.
	 * 
	 * @return the operators
	 */
	public ArrayList<BasicOperator> getOperators() {
		return ops;
	}

	/**
	 * One operator was not supported.
	 * 
	 * @param b
	 *            the b
	 */
	public void oneOperatorWasNotSupported(boolean b) {
		this.oneOperatorWasNotSupported = b;
	}

	/**
	 * Checks if is one operator was not supported.
	 * 
	 * @return true, if is one operator was not supported
	 */
	public boolean isOneOperatorWasNotSupported() {
		return oneOperatorWasNotSupported;
	}
}
