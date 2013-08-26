package lupos.cloud.pig.operator;

import lupos.cloud.pig.PigQuery;

public interface IPigOperator {
	public String buildQuery(PigQuery pigQuery);
}
