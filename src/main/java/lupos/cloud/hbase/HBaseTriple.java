package lupos.cloud.hbase;

public class HBaseTriple {
	String row_key;
	String columnFamily;
	String column;
	String value;
	String tablename;

	public HBaseTriple(String tablename, String row_key, String columnFamily,
			String column, String value) {
		super();
		this.row_key = row_key;
		this.column = column;
		this.columnFamily = columnFamily;
		this.value = value;
		this.tablename = tablename;
	}

	public String getTablename() {
		return tablename;
	}

	public String getRow_key() {
		return row_key;
	}

	public void setRow_key(String row_key) {
		this.row_key = row_key;
	}

	public String getColumn() {
		return column;
	}

	public void setColumn(String column_family) {
		this.column = column_family;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return "HBaseTriple [row_key=" + row_key + ", column_family=" + column
				+ ", value=" + value + "]";
	}

	public String getColumnFamily() {
		return columnFamily;
	}

	public void setColumnFamily(String column_family) {
		this.column = column_family;
	}

}
