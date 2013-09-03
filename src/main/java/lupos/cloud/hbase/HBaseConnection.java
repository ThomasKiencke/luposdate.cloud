package lupos.cloud.hbase;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

import lupos.cloud.hbase.bulkLoad.HBaseKVMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.ipc.SecureRpcEngine.Server;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.hadoopbackport.TotalOrderPartitioner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import au.com.bytecode.opencsv.CSVWriter;

// TODO: Auto-generated Javadoc
/**
 * Diese Klasse stellt die Verbindung mit HBase her. In erster Linie wird sie
 * genutzt um die Verbindung mit HBase herzustellen, die notwendigen Tabellen zu
 * erzeugen und zum hinzufügen von "rows". Beim laden in HBase gibt es zwei
 * Mögliche Modi die mit der Variable MAP_REDUCE_BULK_LOAD definiert werden. Bei
 * größeren Datenmengen bietet sich der "BulkLoad"-Modus an indem die Tripel
 * erst lokal gecacht, und anschließend per Map Reduce Job auf die verschiedenen
 * Knoten verteilt werden.
 */
public class HBaseConnection {

	/** The configuration. */
	static Configuration configuration = null;

	/** The admin. */
	static HBaseAdmin admin = null;

	/** The message. */
	static boolean message = true;

	/** The h tables. */
	static HashMap<String, HTable> hTables = new HashMap<String, HTable>();

	/** The csvwriter. */
	static HashMap<String, CSVWriter> csvwriter = new HashMap<String, CSVWriter>();

	/** The row counter. */
	static int rowCounter = 0;

	/** The Constant ROW_BUFFER_SIZE. */
	public static final int ROW_BUFFER_SIZE = 65000000;

	/** The Constant WORKING_DIR. */
	public static final String WORKING_DIR = "bulkLoadDirectory";

	/** The Constant BUFFER_FILE_NAME. */
	public static final String BUFFER_FILE_NAME = "rowBufferFile";

	/** The Constant BUFFER_HFILE_NAME. */
	public static final String BUFFER_HFILE_NAME = "rowBufferHFile";

	/** The map reduce bulk load. */
	public static boolean MAP_REDUCE_BULK_LOAD = false;

	/** The hdfs_file system. */
	static FileSystem hdfs_fileSystem = null;

	/** The delete table on creation. */
	private static boolean deleteTableOnCreation = false;

	/**
	 * Initialisierung der Verbindung und erstellen der Arbeitsverzeichnisse auf
	 * dem verteilten Dateisystem.
	 * 
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static void init() throws IOException {
		if (configuration == null || admin == null) {
			System.out.println("Verbindung wird aufgebaut ...");
			configuration = HBaseConfiguration.create();
			admin = new HBaseAdmin(configuration);
			System.out
					.print("Verbindung zum Cluster wurde hergestellt. Knoten: ");
			for (ServerName serv : admin.getClusterStatus().getServers()) {
				System.out.print(serv.getHostname() + " ");
			}
			System.out.println();
		}

		if (MAP_REDUCE_BULK_LOAD == true && hdfs_fileSystem == null) {

			hdfs_fileSystem = FileSystem.get(configuration);
			hdfs_fileSystem.delete(new Path("/tmp/" + WORKING_DIR), true);
			hdfs_fileSystem.mkdirs(new Path("/tmp/" + WORKING_DIR));

			new File(WORKING_DIR).mkdir();

		}

	}

	/**
	 * Erzeugen einer Tabelle.
	 * 
	 * @param tablename
	 *            the tablename
	 * @param familyname
	 *            the familyname
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static void createTable(String tablename, String familyname)
			throws IOException {
		init();
		try {
			if (deleteTableOnCreation) {
				deleteTable(tablename);
			}
			HTableDescriptor descriptor = new HTableDescriptor(
					Bytes.toBytes(tablename));
			descriptor.addFamily(new HColumnDescriptor(familyname));
			admin.createTable(descriptor);
			if (message) {
				System.out.println("Tabelle \"" + tablename
						+ "\" wurde erzeugt");
			}
		} catch (TableExistsException e) {
			if (message) {
				System.out.println("Tabelle \"" + tablename
						+ "\" existiert bereits!");
			}
		}
	}

	/**
	 * Die Tripel in dem lokalen TripelCache werden in HBase geladen (nur für
	 * den BulkLoad).
	 */
	public static void flush() {
		rowCounter = 0;
		for (String key : csvwriter.keySet()) {
			try {
				hdfs_fileSystem.copyFromLocalFile(true, true, new Path(
						WORKING_DIR + File.separator + key + "_"
								+ BUFFER_FILE_NAME + ".csv"), new Path("/tmp/"
						+ WORKING_DIR + "/" + key + "_" + BUFFER_FILE_NAME
						+ ".csv"));
				csvwriter.get(key).close();
				bulkLoad(key);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		csvwriter = new HashMap<String, CSVWriter>();
	}

	/**
	 * Fügt eine Spalte zu einer Tballe hinzu.
	 * 
	 * @param tablename
	 *            the tablename
	 * @param columnname
	 *            the columnname
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
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

	/**
	 * Entfernt ein HBase Triple.
	 *
	 * @param tablename the tablename
	 * @param columnFamily the column family
	 * @param rowKey the row key
	 * @param colunmnname the colunmnname
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static void deleteRow(String tablename, String columnFamily,
			String rowKey, String colunmnname) throws IOException {
		init();
		HTable table = hTables.get(tablename);
		if (table == null) {
			table = new HTable(configuration, tablename);
			hTables.put(tablename, table);
		}
		Delete row = new Delete(rowKey.getBytes());
		row.deleteColumn(columnFamily.getBytes(), colunmnname.getBytes());
		table.delete(row);
		if (message) {
			System.out.println(rowKey + " und Spalte " + colunmnname
					+ " wurden gelöscht");
		}
	}

	/**
	 * Deaktivieren einer Tabelle.
	 * 
	 * @param tablename
	 *            the tablename
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
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

	/**
	 * Aktivieren einer Tabelle.
	 * 
	 * @param tablename
	 *            the tablename
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static void enableTable(String tablename) throws IOException {
		init();
		if (!checkTable(tablename))
			return;

		admin.enableTable(tablename);
		if (message) {
			System.out.println("Tabelle \"" + tablename + "\" wurde aktiviert");
		}
	}

	/**
	 * Löschen einer Tabelle. Dazu wird sie erste deaktiviert und anschließend
	 * gelöscht.
	 * 
	 * @param tablename
	 *            the tablename
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static void deleteTable(String tablename) throws IOException {
		init();
		if (!checkTable(tablename))
			return;

		try {
			admin.disableTable(tablename);
		} catch (TableNotEnabledException e) {
			// ignore
		}
		admin.deleteTable(tablename);
		if (message) {
			System.out.println("Tabelle \"" + tablename + "\" wurde gelöscht");
		}
	}

	/**
	 * Gibt alle Tabellen zurück.
	 * 
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static void listAllTables() throws IOException {
		init();
		HTableDescriptor[] list = admin.listTables();
		System.out.println("Tabellen (Anzahl = " + list.length + "):");
		for (HTableDescriptor table : list) {
			System.out.println(table.toString());
		}
	}

	/**
	 * Prüft obn eine Tabelle verfügbar ist.
	 * 
	 * @param tablename
	 *            the tablename
	 * @return true, if successful
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static boolean checkTable(String tablename) throws IOException {
		init();
		if (!admin.isTableAvailable(tablename)) {
			System.out
					.println("Tabelle \"" + tablename + "\" nicht verfügbar!");
			return false;
		}
		return true;
	}

	/**
	 * Fügt eine Reihe (=row) hinzu.
	 * 
	 * @param tablename
	 *            the tablename
	 * @param row_key
	 *            the row_key
	 * @param columnFamily
	 *            the column family
	 * @param column
	 *            the column
	 * @param value
	 *            the value
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static void addRow(String tablename, String row_key,
			String columnFamily, String column, String value)
			throws IOException {
		// schnellere Variante zum einlesen von Tripel
		if (MAP_REDUCE_BULK_LOAD) {
			if (csvwriter.get(tablename) == null) {
				csvwriter.put(tablename, new CSVWriter(new FileWriter(
						WORKING_DIR + File.separator + tablename + "_"
								+ BUFFER_FILE_NAME + ".csv"), '\t'));
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
									+ BUFFER_FILE_NAME + ".csv"), new Path(
							"/tmp/" + WORKING_DIR + "/" + key + "_"
									+ BUFFER_FILE_NAME + ".csv"));
					csvwriter.get(key).close();
					bulkLoad(key);
				}
				csvwriter = new HashMap<String, CSVWriter>();
				hdfs_fileSystem.delete(new Path("/tmp/" + WORKING_DIR), true);
				hdfs_fileSystem.mkdirs(new Path("/tmp/" + WORKING_DIR));
			}

		} else {
			// Einlesen per Tripel per HBase API (sehr langsam bei großen
			// Datenmengen), da die Tripel einzeln übertragen weden.
			HTable table = hTables.get(tablename);
			if (table == null) {
				table = new HTable(configuration, tablename);
				hTables.put(tablename, table);
			}
			Put row = new Put(Bytes.toBytes(row_key));
			row.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
					Bytes.toBytes(value));
			table.put(row);
		}
	}

	/**
	 * Lädt eine Tabelle per Map Reduce Bulkload.
	 * 
	 * @param tablename
	 *            the tablename
	 */
	private static void bulkLoad(String tablename) {
		try {
			System.out.println(tablename + " wird übertragen!");
			// init job
			configuration.set("hbase.table.name", tablename);

			Job job = new Job(configuration, "HBase Bulk Import for "
					+ tablename);
			job.setJarByClass(HBaseKVMapper.class);

			job.setMapperClass(HBaseKVMapper.class);
			job.setMapOutputKeyClass(ImmutableBytesWritable.class);
			job.setMapOutputValueClass(KeyValue.class);
			job.setOutputFormatClass(HFileOutputFormat.class);
			job.setPartitionerClass(TotalOrderPartitioner.class);
			job.setInputFormatClass(TextInputFormat.class);
			TableMapReduceUtil.addDependencyJars(job);

			// generiere HFiles auf dem verteilten Dateisystem
			HTable hTable = new HTable(configuration, tablename);
			HFileOutputFormat.configureIncrementalLoad(job, hTable);
			FileInputFormat.addInputPath(job, new Path("/tmp/" + WORKING_DIR
					+ "/" + tablename + "_" + BUFFER_FILE_NAME + ".csv"));
			FileOutputFormat.setOutputPath(job, new Path("/tmp/" + WORKING_DIR
					+ "/" + tablename + "_" + BUFFER_HFILE_NAME));

			job.waitForCompletion(true);

			// Lade generierte HFiles in HBase
			LoadIncrementalHFiles loader = new LoadIncrementalHFiles(
					configuration);
			loader.doBulkLoad(new Path("/tmp/" + WORKING_DIR + "/" + tablename
					+ "_" + BUFFER_HFILE_NAME), hTable);

		} catch (TableNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Gibt eine Zeile anhand des rowKeys zurück.
	 * 
	 * @param tablename
	 *            the tablename
	 * @param row_key
	 *            the row_key
	 * @return the row
	 */
	public static Result getRow(final String tablename, final String row_key) {
		try {
			init();
			HTable table = hTables.get(tablename);
			if (table == null) {
				table = new HTable(configuration, tablename);
				hTables.put(tablename, table);
			}
			Get g = new Get(Bytes.toBytes(row_key));
			Result result = table.get(g);
			if (result != null) {
				return result;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Gibt eine Zeile anhand des rowkeys und den Prefix einer Spalte zurück.
	 * 
	 * @param tablename
	 *            the tablename
	 * @param row_key
	 *            the row_key
	 * @param cf
	 *            the cf
	 * @param column
	 *            the column
	 * @return the row with column
	 */
	public static Result getRowWithColumn(final String tablename,
			final String row_key, final String cf, final String column) {
		try {
			init();
			HTable table = hTables.get(tablename);
			if (table == null) {
				table = new HTable(configuration, tablename);
				hTables.put(tablename, table);
			}
			Get g = new Get(Bytes.toBytes(row_key));
			g.addFamily(cf.getBytes());
			g.setFilter(new ColumnPrefixFilter(column.getBytes()));
			Result result = table.get(g);
			if (result != null) {
				return result;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Gibt eine Tabelle aus.
	 * 
	 * @param tablename
	 *            the tablename
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
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

	/**
	 * Gibt das Konfigurationsobjekt zurück.
	 * 
	 * @return the configuration
	 */
	public static Configuration getConfiguration() {
		return configuration;
	}
}
