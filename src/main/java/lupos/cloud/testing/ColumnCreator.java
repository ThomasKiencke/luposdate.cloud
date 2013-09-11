package lupos.cloud.testing;

import java.io.IOException;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;


public class ColumnCreator {
	static Configuration configuration = null;

	static HBaseAdmin admin = null;
	static HTable hTable = null;

	public static void main(String[] args) throws IOException {
		if (args.length != 1) {
			System.out.println("java -jar programm <number of psuedo columns");
			System.exit(0);
		}
		// init connection
		configuration = HBaseConfiguration.create();
		admin = new HBaseAdmin(configuration);

		// create testtable
		createTable("mytestTable", "mycf");

		// create pseudodata

		int numberOfColumns = Integer.parseInt(args[0]);

		for (int i = 0; i < numberOfColumns; i++) {
			// empty value and column iterates
			addRow("mytestTable", "sampleRowKey", "mycf", DigestUtils
					.sha512Hex("mycol" + i).toString() + DigestUtils
					.sha512Hex("mycol" + i).toString(), "");
			if (i % 10000 == 0) {
				System.out.println(i + " columns created!");
			}
		}
		
		System.out.println("now testing to scan over the table");
		// finaly scan
		Scan s = new Scan();

		ResultScanner scanner = hTable.getScanner(s);

		for (Result res = scanner.next(); res != null; res = scanner.next()) {

		}
	}

	public static void addRow(String tablename, String row_key,
			String columnFamily, String column, String value)
			throws IOException {

		if (hTable == null) {
			hTable = new HTable(configuration, tablename);
		}
		Put row = new Put(Bytes.toBytes(row_key));
		row.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
				Bytes.toBytes(value));
		hTable.put(row);
	}

	public static void createTable(String tablename, String familyname)
			throws IOException {
		try {
			HTableDescriptor descriptor = new HTableDescriptor(
					Bytes.toBytes(tablename));
			HColumnDescriptor family = new HColumnDescriptor(familyname);
			descriptor.addFamily(family);
			admin.createTable(descriptor);

			System.out.println("Table \"" + tablename + "\" was created");

		} catch (TableExistsException e) {
			System.out.println("Table \"" + tablename + "\" already exists!");
			deleteTable(tablename);
			createTable(tablename, familyname);

		}
	}

	public static void deleteTable(String tablename) throws IOException {
		try {
			admin.disableTable(tablename);
		} catch (TableNotEnabledException e) {
			// ignore
		}
		admin.deleteTable(tablename);
		System.out.println("Table \"" + tablename + "\" removed");

	}
}