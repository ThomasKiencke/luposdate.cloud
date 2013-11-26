package lupos.cloud.operator;

import java.util.ArrayList;

import lupos.engine.operators.BasicOperator;
import lupos.engine.operators.index.BasicIndexScan;

/**
 * Enthält EIN IndexScan-Operator und die darauf folgenden Operationen.
 */
public class IndexScanContainer extends BasicOperator {
	
	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = -5612770902234058839L;
	
	/** id counter . */
	private static int idCounter = 0;
	
	/** The id. */
	private int id;
	
	/** The index scan. */
	private BasicIndexScan indexScan;
	
	/** Folgeoperationen. */
	private ArrayList<BasicOperator> ops = new ArrayList<BasicOperator>();
	
	/** True, wenn eine Operation nicht unterstützt wird. */
	private boolean oneOperatorWasNotSupported;

	/**
	 * Instantiates a new index scan container.
	 *
	 * @param indexScan the index scan
	 */
	public IndexScanContainer(BasicIndexScan indexScan) {
		this.indexScan = indexScan;
		this.id = idCounter;
		idCounter++;
	}

	/**
	 * Fügt einen Operator hinzu.
	 *
	 * @param node the node
	 */
	public void addOperator(BasicOperator node) {
		this.ops.add(node);
	}

	/* (non-Javadoc)
	 * @see lupos.engine.operators.BasicOperator#toString()
	 */
	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
		result.append("--- IndexScanContainer (" + indexScan.getTriplePattern().size() + ")  --- \n");
		for (BasicOperator op : ops) {
			result.append(op.getClass().getSimpleName() + "\n");
		}

		return result.toString();
	}
	
	/**
	 * Gibt den IndexScan-Operator zurück.
	 *
	 * @return the index scan
	 */
	public BasicIndexScan getIndexScan() {
		return indexScan;
	}
	
	/**
	 * Gets the id.
	 *
	 * @return the id
	 */
	public int getId() {
		return id;
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
	 * @param b the b
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
