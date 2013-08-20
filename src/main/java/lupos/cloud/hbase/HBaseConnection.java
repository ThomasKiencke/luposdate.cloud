package lupos.cloud.hbase;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Scanner;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import lupos.cloud.hbase.bulkLoad.HBaseKVMapper;
import lupos.cloud.pig.udfs.MapToBag;
import lupos.cloud.pig.udfs.PigLoadUDF;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.hadoopbackport.TotalOrderPartitioner;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import au.com.bytecode.opencsv.CSVParser;
import au.com.bytecode.opencsv.CSVWriter;

public class HBaseConnection {
	static Configuration configuration = null;
	static HBaseAdmin admin = null;
	static boolean message = false;
	static String COLUMN_FAMILY = HBaseTableStrategy.getTableInstance()
			.getColumnFamilyName();
	static HashMap<String, HTable> hTables = new HashMap<String, HTable>();
	static HashMap<String, CSVWriter> csvwriter = new HashMap<String, CSVWriter>();
	static int rowCounter = 0;
	public static final int ROW_BUFFER_SIZE = 1000;
	public static final String WORKING_DIR = "bulkLoadDirectory";
	public static final String BUFFER_FILE_NAME = "rowBufferFile";
	public static final String BUFFER_HFILE_NAME = "rowBufferHFile";
	static int file_counter = 0;
	static final boolean MAP_REDUCE_BULK_LOAD = true;
	static FileSystem hdfs_fileSystem = null;
	
	public static void init() throws IOException {
		if (configuration == null || admin == null) {
			configuration = HBaseConfiguration.create();
			// configuration = new Configuration(false);
			// configuration.set("fs.defaultFS", "hdfs://localhost:8020");
			// configuration.set("fs.default.name", "hdfs://localhost:8020");
			// configuration.set("mapred.job.tracker", "localhost:8021");
			// configuration.set("hbase.zookeeper.quorum", "localhost");
			// configuration.set("hbase.zookeeper.property.clientPort", "2181");

			// configuration.set("tmpjars",
			// "hdfs://192.168.2.41:8020/tmp/hbase-0.94.6-cdh4.3.0.jar");
			// TableMapReduceUtil.addDependencyJars(configuration,
//			TableMapReduceUtil.addDependencyJars(arg0);
			// HFileOutputFormat.class);
			admin = new HBaseAdmin(configuration);
			if (MAP_REDUCE_BULK_LOAD) {
				// Configuration hdfsConf = new Configuration();
				configuration.set("libjars", "libjars/");
				TableMapReduceUtil.addDependencyJars(configuration,
						HBaseKVMapper.class, CSVParser.class, HFileOutputFormat.class);
				// configuration.set("fs.defaultFS", "hdfs://localhost:8020/");
				hdfs_fileSystem = FileSystem.get(configuration);
				hdfs_fileSystem.delete(new Path("/tmp/" + WORKING_DIR), true);
				hdfs_fileSystem.mkdirs(new Path("/tmp/" + WORKING_DIR));

				// hdfsConf.set("hadoop.job.ugi", "hbase");
				new File(WORKING_DIR).mkdir();
			}
		}
	}

	public static void createTable(String tablename, String familyname)
			throws IOException {
		init();
		try {
//			 deleteTable(tablename);
			HTableDescriptor descriptor = new HTableDescriptor(
					Bytes.toBytes(tablename));
//			 deleteTable(tablename);
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

	public static void flush() {
		rowCounter = 0;
		for (String key : csvwriter.keySet()) {
			try {
				hdfs_fileSystem.copyFromLocalFile(true, true, new Path(
						WORKING_DIR + File.separator + key + "_"
								+ BUFFER_FILE_NAME + "_" + file_counter
								+ ".csv"), new Path("/tmp/" + WORKING_DIR + "/"
						+ key + "_" + BUFFER_FILE_NAME + "_" + file_counter
						+ ".csv"));
				csvwriter.get(key).close();
				bulkLoad(key);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		csvwriter = new HashMap<String, CSVWriter>();
		file_counter++;
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
					+ "\" hinzugefügt!");
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
			System.out.println("Tabelle \"" + tablename + "\" wurde gelöscht");
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
			System.out
					.println("Tabelle \"" + tablename + "\" nicht verfügbar!");
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

	public static void addRow(String tablename, String row_key,
			String columnFamily, String column, String value)
			throws IOException {
		// byte[] databytes = Bytes.toBytes("data");
		// p1.add(databytes, Bytes.toBytes("1"), Bytes.toBytes("value1"));
		// table.put(p1);

		if (MAP_REDUCE_BULK_LOAD) {
			// schnellere Variante zum einlesen von Tripel
			if (csvwriter.get(tablename) == null) {
				csvwriter.put(tablename, new CSVWriter(new FileWriter(
						WORKING_DIR + File.separator + tablename + "_"
								+ BUFFER_FILE_NAME + "_" + file_counter
								+ ".csv"), '\t'));
			}
			// Schreibe die Zeile in auf den Festplattenpuffer
			String[] entries = { columnFamily, row_key, column, value };
			csvwriter.get(tablename).writeNext(entries);
			rowCounter++;

			if (rowCounter == ROW_BUFFER_SIZE) {
				rowCounter = 0;
				for (String key : csvwriter.keySet()) {
					hdfs_fileSystem.copyFromLocalFile(true, true, new Path(
							WORKING_DIR + File.separator + key + "_"
									+ BUFFER_FILE_NAME + "_" + file_counter
									+ ".csv"), new Path("/tmp/" + WORKING_DIR
							+ "/" + key + "_" + BUFFER_FILE_NAME + "_"
							+ file_counter + ".csv"));
					csvwriter.get(key).close();
					bulkLoad(key);
					// Scanner sc = new Scanner(System.in);
					// System.out.println("wait for ENTER");
					// sc.nextLine();
					// new File(WORKING_DIR + tablename + "_" +
					// BUFFER_FILE_NAME)
					// .delete();
					// new File(WORKING_DIR + tablename + "_" +
					// BUFFER_FILE_NAME)
					// .delete();
				}
				csvwriter = new HashMap<String, CSVWriter>();
				file_counter++;
			}

		} else {
			// Einlesen per Tripel per HBase API (sehr langsam bei großen
			// Datenmengen)
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
	}

	private static void bulkLoad(String tablename) {
		try {
			// conf.setInt("epoch.seconds.tipoff", 1275613200);
			configuration.set("hbase.table.name", tablename);

			// TableMapReduceUtil.addDependencyJars(configuration,
			// HFileOutputFormat.class);

			// Load hbase-site.xml
			// HBaseConfiguration.addHbaseResources(conf);

			Job job = new Job(configuration, "HBase Bulk Import for "
					+ tablename + "(" + file_counter + ")");
			// createJarForClass(HBaseKVMapper.class);
			job.setJarByClass(HBaseKVMapper.class);
			// job.setJarByClass(HFileOutputFormat.class);

			job.setMapperClass(HBaseKVMapper.class);
			job.setMapOutputKeyClass(ImmutableBytesWritable.class);
			job.setMapOutputValueClass(KeyValue.class);
			job.setOutputFormatClass(HFileOutputFormat.class);
			job.setPartitionerClass(TotalOrderPartitioner.class);
			job.setInputFormatClass(TextInputFormat.class);
			
//			TableMapReduceUtil.addDependencyJars(job);
//			TableMapReduceUtil.addDependencyJars(configuration,
//					HBaseKVMapper.class);
			// DistributedCache.addFileToClassPath(new
			// Path("/tmp/hbase-0.94.6-cdh4.3.0.jar"), job.getConfiguration());
			// Configuration hConf = HBaseConfiguration.create(configuration);
			// hConf.set("hbase.zookeeper.quorum", "127.0.0.1");
			// hConf.set("hbase.zookeeper.property.clientPort",
			// "2181");

			// TableMapReduceUtil.initTableReducerJob(tablename, null, job);
			// TableMapReduceUtil.addDependencyJars(job);
			// TableMapReduceUtil.addDependencyJars(job.getConfiguration());

			HTable hTable = new HTable(configuration, tablename);

			// Auto configure partitioner and reducer
			HFileOutputFormat.configureIncrementalLoad(job, hTable);

			FileInputFormat.addInputPath(job, new Path("/tmp/" + WORKING_DIR
					+ "/" + tablename + "_" + BUFFER_FILE_NAME + "_"
					+ file_counter + ".csv"));
			FileOutputFormat.setOutputPath(job, new Path("/tmp/" + WORKING_DIR
					+ "/" + tablename + "_" + BUFFER_HFILE_NAME + "_"
					+ file_counter));

			// TableMapReduceUtil.addDependencyJars(job);
			// TableMapReduceUtil.addDependencyJars(job.getConfiguration(),
			// org.apache.hadoop.hbase.mapreduce.HFileOutputFormat.class);
			job.waitForCompletion(true);

			// Load generated HFiles into table
			LoadIncrementalHFiles loader = new LoadIncrementalHFiles(
					configuration);
			loader.doBulkLoad(new Path("/tmp/" + WORKING_DIR + "/" + tablename
					+ "_" + BUFFER_HFILE_NAME + "_" + file_counter), hTable);
		} catch (TableNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
