package lupos.cloud.hbase;

import java.util.Collection;
import java.util.TreeMap;

import lupos.datastructures.items.Triple;

/**
 * Oberkalsse für die HBase Verteilungsstrategie der Tripel.
 */
public abstract class HBaseDistributionStrategy {
	
	/** The table strategy. */
	public static int TABLE_STRATEGY = HexaDistributionTableStrategy.STRAGEGY_ID;
	
	/** The instance. */
	private static HBaseDistributionStrategy instance = null;
	
	/**
	 * Gibt alle Tabellennamen zurück.
	 *
	 * @return the table names
	 */
	public abstract String[] getTableNames();
	
	/**
	 * Gibt die Strategie Instanz zurück.
	 *
	 * @return the table instance
	 */
	public static HBaseDistributionStrategy getTableInstance() {
		if (instance == null) {
			switch (TABLE_STRATEGY) {
			case HexaDistributionTableStrategy.STRAGEGY_ID:
				instance = new HexaDistributionTableStrategy();
				break;
//			case Strategy2HBaseTableStrategy.STRAGEGY_ID:
//				instance = new Strategy2HBaseTableStrategy();
//				break;
			default:
				instance = new HexaDistributionTableStrategy();
				break;
			}
		}
		return instance;
	}

	/**
	 * Generiert anhand eines Tripel die verschiedenen Indizes.
	 *
	 * @param triple the triple
	 * @return the collection
	 */
	public abstract Collection<HBaseTriple> generateIndecesTriple(
			final Triple triple);

	/**
	 * Gibt die Reihenfolge der Elemente wieder.
	 *
	 * @param elements the elements
	 * @param triple the triple
	 * @return the input value
	 */
	public abstract TreeMap<Integer, Object> getInputValue(String elements,
			Triple triple);

	/**
	 * Generiert aus einem Tripel ein HBase Tripel.
	 *
	 * @param tablename the tablename
	 * @param row_key the row_key
	 * @param column the column
	 * @param value the value
	 * @return the h base triple
	 */
	public abstract HBaseTriple generateHBaseTriple(final String tablename,
			final String row_key, final String column, final String value);
	
	/**
	 * Gibt den Namen der Spaltenfamilie zurück.
	 *
	 * @return the column family name
	 */
	public abstract String getColumnFamilyName();

}
