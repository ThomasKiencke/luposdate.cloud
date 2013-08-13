package lupos.cloud.hbase;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import lupos.misc.FileHelper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseConnection {
	static Configuration configuration = null;
	static HBaseAdmin admin = null;
	static boolean message = false;
	static String COLUMN_FAMILY = "VALUE";
	static HashMap<String, HTable> hTables = new HashMap<String, HTable>();

	public static void init() throws IOException {
		if (configuration == null || admin == null) {
			configuration = HBaseConfiguration.create();
			admin = new HBaseAdmin(configuration);
		}
	}

	public static void createTable(String tablename, String familyname)
			throws IOException {
		init();
		try {
			HTableDescriptor descriptor = new HTableDescriptor(
					Bytes.toBytes(tablename));
			descriptor.addFamily(new HColumnDescriptor(familyname));
			admin.createTable(descriptor);
			if (message) {
				System.out.println("Tabelle \"" + tablename
						+ "\" wurde erzeugt");
			}
		} catch (TableExistsException e) {
			System.out.println("Tabelle \"" + tablename
					+ "\" existiert bereits!");
		}
	}

	public static void addColumn(String tablename, String columnname)
			throws IOException {
		init();
		if (!checkTable(tablename))
			return;

		admin.addColumn(tablename, new HColumnDescriptor(columnname));
		if (message) {
			System.out.println("Spalte \"" + columnname
					+ "\" wurde in die Tabelle \"" + tablename
					+ "\" hinzugef�gt!");
		}
	}

	public static void deleteColumn(String tablename, String colunmnname)
			throws IOException {
		init();
		if (!checkTable(tablename))
			return;

		admin.deleteColumn(tablename, colunmnname);
		if (message) {
			System.out.println("Deleted column : " + colunmnname
					+ "from table " + tablename);
		}
	}

	public static void disableTable(String tablename) throws IOException {
		init();
		if (!checkTable(tablename))
			return;

		admin.disableTable(tablename);
		if (message) {
			System.out.println("Tabelle \"" + tablename
					+ "\" wurde deaktiviert");
		}
	}

	public static void enableTable(String tablename) throws IOException {
		init();
		if (!checkTable(tablename))
			return;

		admin.enableTable(tablename);
		if (message) {
			System.out.println("Tabelle \"" + tablename + "\" wurde aktiviert");
		}
	}

	public static void deleteTable(String tablename) throws IOException {
		init();
		if (!checkTable(tablename))
			return;

		admin.disableTable(tablename);
		admin.deleteTable(tablename);
		if (message) {
			System.out
					.println("Tabelle \"" + tablename + "\" wurde gel�scht");
		}
	}

	public static void listAllTables() throws IOException {
		init();
		HTableDescriptor[] list = admin.listTables();
		System.out.println("Tabellen (Anzahl = " + list.length + "):");
		for (HTableDescriptor table : list) {
			System.out.println(table.toString());
		}
	}

	public static boolean checkTable(String tablename) throws IOException {
		init();
		if (!admin.isTableAvailable(tablename)) {
			System.out.println("Tabelle \"" + tablename
					+ "\" nicht verf�gbar!");
			return false;
		}
		return true;
	}

	public static void flush(String tablename) {
		try {
			init();
			admin.flush(tablename);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void addRow(String tablename, String row_key, String column, String value)
			throws IOException {
		// byte[] databytes = Bytes.toBytes("data");
		// p1.add(databytes, Bytes.toBytes("1"), Bytes.toBytes("value1"));
		// table.put(p1);
		HTable table = hTables.get(tablename);
		if (table == null) {
			table = new HTable(configuration, tablename);
			hTables.put(tablename, table);
		}
		Put row = new Put(Bytes.toBytes(row_key));
		row.add(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(column),
				Bytes.toBytes(value));
		table.put(row);
	}

	public static void getRow(final String tablename, final String row_key) {
		try {
			HTable table = hTables.get(tablename);
			if (table == null) {
				table = new HTable(configuration, tablename);
				hTables.put(tablename, table);
			}
			Get g = new Get(Bytes.toBytes(row_key));
			Result result = table.get(g);
			if (result != null)
				System.out.println("Get: " + result);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void printTable(String tablename) throws IOException {
		init();
		HTable table = new HTable(configuration, tablename);
		ResultScanner scanner = table.getScanner(new Scan());
		try {
			for (Result scannerResult : scanner) {
				System.out.println("row_key: " + scannerResult.getRow());

			}
		} finally {
			scanner.close();
		}
	}
}
