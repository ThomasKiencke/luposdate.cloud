package lupos.cloud.testing;

public class CloudBitvector {
	String row;
	byte[] columnFamily;
	private String tablename;
	Integer patternId = null;
	Integer setId = null;
			
	
	public CloudBitvector(String tablename, String row, byte[] columnFamily, Integer patternId) {
		super();
		this.tablename = tablename;
		this.row = row;
		this.columnFamily = columnFamily;
		this.patternId = patternId;
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
	
	public Integer getPatternId() {
		return patternId;
	}
}
