package lupos.cloud.operator;

import java.util.ArrayList;

import lupos.engine.operators.BasicOperator;
import lupos.engine.operators.index.BasicIndexScan;

public class IndexScanContainer extends BasicOperator {
	private static final long serialVersionUID = -5612770902234058839L;
	private static int idCounter = 0;
	private int id;
	private BasicIndexScan indexScan;
	private ArrayList<BasicOperator> ops = new ArrayList<BasicOperator>();

	public IndexScanContainer(BasicIndexScan indexScan) {
		this.indexScan = indexScan;
		this.id = idCounter;
		idCounter++;
	}

	public void addOperator(BasicOperator node) {
		this.ops.add(node);
	}

	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
		result.append("--- IndexScanContainer ---\n");
//		for (BasicOperator curNode : ops) {
//			result.append("\n" + curNode.getClass().getSimpleName());
//
//		}

		return result.toString();
	}
	
	public BasicIndexScan getIndexScan() {
		return indexScan;
	}
	
	public int getId() {
		return id;
	}
	
	public ArrayList<BasicOperator> getOperators() {
		return ops;
	}

}
