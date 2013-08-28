package lupos.cloud.operator;

import java.util.ArrayList;
import java.util.List;

import lupos.cloud.operator.format.AddCloudProjection;
import lupos.datastructures.bindings.Bindings;
import lupos.datastructures.queryresult.QueryResult;
import lupos.engine.operators.BasicOperator;
import lupos.engine.operators.Operator;
import lupos.engine.operators.OperatorIDTuple;
import lupos.engine.operators.RootChild;
import lupos.engine.operators.index.BasicIndexScan;
import lupos.engine.operators.index.Dataset;
import lupos.engine.operators.index.Indices;
import lupos.engine.operators.index.Root;
import lupos.engine.operators.singleinput.Projection;
import lupos.engine.operators.singleinput.filter.Filter;

public class CloudUnion extends BasicOperator {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5612770902234058839L;

	ArrayList<BasicIndexScan> indexScanList = new ArrayList<BasicIndexScan>();

	public void addOperator(BasicIndexScan op) {
		indexScanList.add(op);
	}

	public ArrayList<BasicIndexScan> getOperatorList() {
		return indexScanList;
	}

	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
		result.append("--- Cloud Union ---\n");
		for (BasicOperator curNode : indexScanList) {
			result.append("\n" + curNode.getClass().getSimpleName());

		}

		return result.toString();
	}
	
	public ArrayList<BasicIndexScan> getIndexScanList() {
		return indexScanList;
	}
}
