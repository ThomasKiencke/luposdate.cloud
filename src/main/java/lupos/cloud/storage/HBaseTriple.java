package lupos.cloud.storage;

public class HBaseTriple {
	String row_key;
	String column_family;
	String value;

	public HBaseTriple(String row_key, String column_family, String value) {
		super();
		this.row_key = row_key;
		this.column_family = column_family;
		this.value = value;
	}

	public String getRow_key() {
		return row_key;
	}

	public void setRow_key(String row_key) {
		this.row_key = row_key;
	}

	public String getColumn_family() {
		return column_family;
	}

	public void setColumn_family(String column_family) {
		this.column_family = column_family;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return "HBaseTriple [row_key=" + row_key + ", column_family="
				+ column_family + ", value=" + value + "]";
	}
	
}
