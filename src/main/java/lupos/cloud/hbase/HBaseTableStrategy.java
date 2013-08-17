package lupos.cloud.hbase;

import java.util.Collection;
import java.util.TreeMap;

import lupos.datastructures.items.Triple;

public abstract class HBaseTableStrategy {
	
	public static int TABLE_STRATEGY = Strategy1HBaseTableStrategy.STRAGEGY_ID;
	private static HBaseTableStrategy instance = null;
	
	public abstract String[] getTableNames();
	
	public static HBaseTableStrategy getTableInstance() {
		if (instance == null) {
			switch (TABLE_STRATEGY) {
			case Strategy1HBaseTableStrategy.STRAGEGY_ID:
				instance = new Strategy1HBaseTableStrategy();
				break;
			case Strategy2HBaseTableStrategy.STRAGEGY_ID:
				instance = new Strategy2HBaseTableStrategy();
				break;
			default:
				instance = new Strategy1HBaseTableStrategy();
				break;
			}
		}
		return instance;
	}

	public abstract Collection<HBaseTriple> generateSixIndecesTriple(
			final Triple triple);

	public abstract TreeMap<Integer, Object> getInputValue(String elements,
			Triple triple);

	public abstract HBaseTriple generateHBaseTriple(final String tablename,
			final String row_key, final String column, final String value);
	
	public abstract String getColumnFamilyName();

}
