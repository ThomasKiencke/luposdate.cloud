package lupos.cloud.testing;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;

import au.com.bytecode.opencsv.CSVWriter;

public class ScannerTest {
	static Configuration configuration = null;

	static HBaseAdmin admin = null;
	static HTable hTable = null;

	public static void main(String[] args) throws IOException {
		// init connection
		configuration = HBaseConfiguration.create();
		admin = new HBaseAdmin(configuration);

		hTable = new HTable(configuration, "mytestTable");

		// finaly scan
		Scan s = new Scan();

		ResultScanner scanner = hTable.getScanner(s);

		for (Result res = scanner.next(); res != null; res = scanner.next()) {

		}
		System.out.println("ready");

	}
}