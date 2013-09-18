package lupos.cloud.testing;

public class CloudBitvector {
	String row;
	String columnFamily;
	private String tablename;
	
	public CloudBitvector(String tablename, String row, String columnFamily) {
		super();
		this.tablename = tablename;
		this.row = row;
		this.columnFamily = columnFamily;
	}
	
	public String getColumnFamily() {
		return columnFamily;
	}
	
	public String getRow() {
		return row;
	}
	
	public String getTablename() {
		return tablename;
	}
}
