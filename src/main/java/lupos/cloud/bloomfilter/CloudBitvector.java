package lupos.cloud.bloomfilter;

/**
 * Für jedes Tripel-Muster wird ein "CloudBitvector" erzeugt in dem die
 * Informationen gespeichert werden die für das Laden des Bitvektors aus HBase
 * später wichtig sind.
 */
public class CloudBitvector {

	/** The row. */
	String row;

	/** The column family. */
	byte[] columnFamily;

	/** The tablename. */
	private String tablename;

	/** The pattern id. */
	Integer patternId = null;

	/** The set id. */
	Integer setId = null;

	/**
	 * Instantiates a new cloud bitvector.
	 * 
	 * @param tablename
	 *            the tablename
	 * @param row
	 *            the row
	 * @param columnFamily
	 *            the column family
	 * @param patternId
	 *            the pattern id
	 */
	public CloudBitvector(String tablename, String row, byte[] columnFamily,
			Integer patternId) {
		super();
		this.tablename = tablename;
		this.row = row;
		this.columnFamily = columnFamily;
		this.patternId = patternId;
		setId = 0;
	}

	/**
	 * Gets the column family.
	 * 
	 * @return the column family
	 */
	public byte[] getColumnFamily() {
		return columnFamily;
	}

	/**
	 * Gets the row.
	 * 
	 * @return the row
	 */
	public String getRow() {
		return row;
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
	 * Sets the inc.
	 */
	public void setInc() {
		this.setId++;
	}

	/**
	 * Gets the sets the id.
	 * 
	 * @return the sets the id
	 */
	public Integer getSetId() {
		return setId;
	}

	/**
	 * Gets the pattern id.
	 * 
	 * @return the pattern id
	 */
	public Integer getPatternId() {
		return patternId;
	}
}
