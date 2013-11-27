package lupos.cloud.pig.udfs;

import org.apache.pig.FilterFunc;

import org.apache.pig.backend.executionengine.ExecException;

import org.apache.pig.data.Tuple;

/**
 * UDF für SPARQL bound()-Funktion
 */
public class BoundFilterUDF extends FilterFunc {

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.pig.EvalFunc#exec(org.apache.pig.data.Tuple)
	 */
	@Override
	public Boolean exec(Tuple input) throws ExecException {
		Object element = input.get(0);
		if (element == null) {
			return false;
		} else {
			return true;
		}
	}

}
