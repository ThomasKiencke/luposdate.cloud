package lupos.cloud.operator;

import java.util.ArrayList;

import lupos.datastructures.bindings.Bindings;
import lupos.datastructures.queryresult.QueryResult;
import lupos.engine.operators.BasicOperator;
import lupos.engine.operators.Operator;
import lupos.engine.operators.RootChild;
import lupos.engine.operators.index.BasicIndexScan;
import lupos.engine.operators.index.Dataset;
import lupos.engine.operators.index.Indices;
import lupos.engine.operators.index.Root;

public class CloudJoin extends BasicOperator {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -5612770902234058839L;
	
	ArrayList<BasicOperator> indexScanList = new ArrayList<BasicOperator>();
	
	public void addOperator(BasicOperator op) {
		indexScanList.add(op);
	}

	public ArrayList<BasicOperator> getOperatorList() {
		return indexScanList;
	}
	
	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
		result.append("--- Cloud Join ---\n");
		for (BasicOperator curNode : indexScanList) {
			result.append("\n" + curNode.getClass().getSimpleName());

		}

		return result.toString();
	}
}
