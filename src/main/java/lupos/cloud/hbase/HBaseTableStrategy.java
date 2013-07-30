package lupos.cloud.hbase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.TreeMap;

import lupos.datastructures.items.Triple;

public class HBaseTableStrategy {

	public static final String[] TABLE_NAMES = { "S_PO", "P_SO", "O_SP",
			"PS_O", "SO_P", "PO_S" };

	public static Collection<HBaseTriple> generateSixIndecesTriple(
			final Triple triple) {
		ArrayList<HBaseTriple> result = new ArrayList<HBaseTriple>();
		for (String tablename : TABLE_NAMES) {
			String row_key_string = tablename.substring(0, tablename.indexOf("_"));
			String column_name_string = tablename.substring(tablename.indexOf("_") + 1,
					tablename.length());

			String row_key = "";
			for (Integer key : getInputValue(row_key_string, triple).keySet()) {
				row_key += getInputValue(row_key_string, triple).get(key);
			}

			String column = "";
			for (Integer key : getInputValue(column_name_string, triple)
					.keySet()) {
				column += getInputValue(column_name_string, triple).get(
						key);
			}

			result.add(generateHBaseTriple(tablename, row_key, column, ""));
		}
		return result;
	}

	public static TreeMap<Integer, Object> getInputValue(String elements,
			Triple triple) {
		TreeMap<Integer, Object> tm = new TreeMap<Integer, Object>();
		int subject = elements.indexOf('S');
		if (subject > -1)
			tm.put(subject, triple.getSubject());
		int predicate = elements.indexOf('P');
		if (predicate > -1)
			tm.put(predicate, triple.getPredicate());
		int object = elements.indexOf('O');
		if (object > -1)
			tm.put(object, triple.getObject());
		return tm;
	}

	public static HBaseTriple generateHBaseTriple(final String tablename, final String row_key,
			final String column, final String value) {
		return new HBaseTriple(tablename, row_key, column, value);
	}

}
