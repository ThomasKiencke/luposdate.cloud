package lupos.cloud.testing;

public class CloudBitvector {
	String row;
	byte[] columnFamily;
	private String tablename;
	private static int idCounter = 0;
	Integer id = null;
	Integer setId = null;
			
	
	public CloudBitvector(String tablename, String row, byte[] columnFamily) {
		super();
		this.tablename = tablename;
		this.row = row;
		this.columnFamily = columnFamily;
		this.id = idCounter;
		idCounter++;
		setId = 0;
	}
	
	public byte[] getColumnFamily() {
		return columnFamily;
	}
	
	public String getRow() {
		return row;
	}
	
	public String getTablename() {
		return tablename;
	}
	
//	public Integer getId() {
//		return id;
//	}
	
	public void setInc() {
		this.setId++;
	}
	
	public Integer getSetId() {
		return setId;
	}
}
