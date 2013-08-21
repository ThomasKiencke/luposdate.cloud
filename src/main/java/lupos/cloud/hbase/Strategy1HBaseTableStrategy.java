package lupos.cloud.hbase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.TreeMap;

import lupos.datastructures.items.Triple;

public class Strategy1HBaseTableStrategy extends HBaseTableStrategy {
	public static final int STRAGEGY_ID = 1;
	public static final String COLUMN_FAMILY = "VALUE";

	public String[] getTableNames() {
		String[] result = { "S_PO", "P_SO", "O_SP", "PS_O", "SO_P", "PO_S" };
		return result;
	}

	public Collection<HBaseTriple> generateSixIndecesTriple(final Triple triple) {
		ArrayList<HBaseTriple> result = new ArrayList<HBaseTriple>();
		for (String tablename : getTableNames()) {
			String row_key_string = tablename.substring(0,
					tablename.indexOf("_"));
			String column_name_string = tablename.substring(
					tablename.indexOf("_") + 1, tablename.length());

			String row_key = "";
			boolean first = true;
			for (Integer key : getInputValue(row_key_string, triple).keySet()) {
				if (first) {
					first = false;
				} else {
					row_key += ",";
				}
				row_key += getInputValue(row_key_string, triple).get(key);
			}

			String column = "";
			first = true;
			for (Integer key : getInputValue(column_name_string, triple)
					.keySet()) {
				if (first) {
					first = false;
				} else {
					column += ",";
				}
				column += getInputValue(column_name_string, triple).get(key);
			}
			result.add(generateHBaseTriple(tablename, row_key, column, ""));
		}
		return result;
	}

	public TreeMap<Integer, Object> getInputValue(String elements, Triple triple) {
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

	public HBaseTriple generateHBaseTriple(final String tablename,
			final String row_key, final String column, final String value) {
		return new HBaseTriple(tablename, row_key, COLUMN_FAMILY, column, value);
	}

	@Override
	public String getColumnFamilyName() {
		return COLUMN_FAMILY;
	}
}
