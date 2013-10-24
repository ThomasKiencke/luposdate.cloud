package lupos.cloud.pig.udfs;


import org.apache.pig.FilterFunc;

import org.apache.pig.backend.executionengine.ExecException;

import org.apache.pig.data.Tuple;


public class BoundFilterUDF extends FilterFunc {

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
