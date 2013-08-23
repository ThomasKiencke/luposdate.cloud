package lupos.cloud.hbase;

/**
 * Speichert die Informationen eines HBase Tripels <=> einer Zeile in HBase.
 */
public class HBaseTriple {

	/** The row_key. */
	String row_key;

	/** The column family. */
	String columnFamily;

	/** The column. */
	String column;

	/** The value. */
	String value;

	/** The tablename. */
	String tablename;

	/**
	 * Instantiates a new h base triple.
	 * 
	 * @param tablename
	 *            the tablename
	 * @param row_key
	 *            the row_key
	 * @param columnFamily
	 *            the column family
	 * @param column
	 *            the column
	 * @param value
	 *            the value
	 */
	public HBaseTriple(String tablename, String row_key, String columnFamily,
			String column, String value) {
		super();
		this.row_key = row_key;
		this.column = column;
		this.columnFamily = columnFamily;
		this.value = value;
		this.tablename = tablename;
	}

	/**
	 * Gets the tablename.
	 * 
	 * @return the tablename
	 */
	public String getTablename() {
		return tablename;
	}

	/**
	 * Gets the row_key.
	 * 
	 * @return the row_key
	 */
	public String getRow_key() {
		return row_key;
	}

	/**
	 * Sets the row_key.
	 * 
	 * @param row_key
	 *            the new row_key
	 */
	public void setRow_key(String row_key) {
		this.row_key = row_key;
	}

	/**
	 * Gets the column.
	 * 
	 * @return the column
	 */
	public String getColumn() {
		return column;
	}

	/**
	 * Sets the column.
	 * 
	 * @param column_family
	 *            the new column
	 */
	public void setColumn(String column_family) {
		this.column = column_family;
	}

	/**
	 * Gets the value.
	 * 
	 * @return the value
	 */
	public String getValue() {
		return value;
	}

	/**
	 * Sets the value.
	 * 
	 * @param value
	 *            the new value
	 */
	public void setValue(String value) {
		this.value = value;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "HBaseTriple [row_key=" + row_key + ", column_family=" + column
				+ ", value=" + value + "]";
	}

	/**
	 * Gets the column family.
	 * 
	 * @return the column family
	 */
	public String getColumnFamily() {
		return columnFamily;
	}

	/**
	 * Sets the column family.
	 * 
	 * @param column_family
	 *            the new column family
	 */
	public void setColumnFamily(String column_family) {
		this.column = column_family;
	}

}
