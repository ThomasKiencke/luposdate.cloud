package lupos.cloud.testing;

public class CloudBitvector {
	String row;
	String columnFamily;
	private String tablename;
	private static int idCounter = 0;
	Integer id = null;
	
	public CloudBitvector(String tablename, String row, String columnFamily) {
		super();
		this.tablename = tablename;
		this.row = row;
		this.columnFamily = columnFamily;
		this.id = idCounter;
		idCounter++;
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
	
	public Integer getId() {
		return id;
	}
}
