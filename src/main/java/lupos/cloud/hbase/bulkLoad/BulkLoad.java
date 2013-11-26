package lupos.cloud.hbase.bulkLoad;

import java.io.IOException;

import lupos.cloud.hbase.HBaseConnection;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.hadoopbackport.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Für das Übertragen der Tripel wird für jede Tabelle ein eigener Thread
 * gestartet.
 */
public class BulkLoad extends Thread {

	/** Name der Tabelle. */
	private String tablename;

	/** Job Referenz. */
	private Job job;

	/** Job Status. */
	private boolean finished = false;

	/**
	 * Instantiates a new bulk load.
	 * 
	 * @param tablename
	 *            the tablename
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public BulkLoad(String tablename) throws IOException {
		this.tablename = tablename;
		createJob();
	}

	/**
	 * Erzeugt den Job und übergibt alle nötigen Informationen.
	 * 
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	private void createJob() throws IOException {
		System.out.println(tablename + " wird uebertragen!");
		// init job
		HBaseConnection.getConfiguration().set("hbase.table.name", tablename);

		job = new Job(HBaseConnection.getConfiguration(),
				"HBase Bulk Import for " + tablename);
		job.setJarByClass(HBaseKVMapper.class);

		job.setMapperClass(HBaseKVMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(KeyValue.class);
		job.setOutputFormatClass(HFileOutputFormat.class);
		job.setPartitionerClass(TotalOrderPartitioner.class);
		job.setInputFormatClass(TextInputFormat.class);

		// TableMapReduceUtil.addDependencyJars(job);

	}

	/**
	 * Lädt eine Tabelle per Map Reduce Bulkload.
	 * 
	 */

	public void run() {
		try {
			// generiere HFiles auf dem verteilten Dateisystem
			HTable hTable = new HTable(HBaseConnection.getConfiguration(),
					tablename);

			HFileOutputFormat.configureIncrementalLoad(job, hTable);

			FileInputFormat.addInputPath(job, new Path("/tmp/"
					+ HBaseConnection.WORKING_DIR + "/" + tablename + "_"
					+ HBaseConnection.BUFFER_FILE_NAME + ".csv"));
			FileOutputFormat.setOutputPath(job, new Path("/tmp/"
					+ HBaseConnection.WORKING_DIR + "/" + tablename + "_"
					+ HBaseConnection.BUFFER_HFILE_NAME));

			job.waitForCompletion(true);

			// Lade generierte HFiles in HBase
			LoadIncrementalHFiles loader = new LoadIncrementalHFiles(
					HBaseConnection.getConfiguration());
			loader.doBulkLoad(
					new Path("/tmp/" + HBaseConnection.WORKING_DIR + "/"
							+ tablename + "_"
							+ HBaseConnection.BUFFER_HFILE_NAME), hTable);

			this.finished = true;

		} catch (TableNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Gets the job.
	 * 
	 * @return the job
	 */
	public Job getJob() {
		return job;
	}

	/**
	 * Checks if is finished.
	 * 
	 * @return true, if is finished
	 */
	public boolean isFinished() {
		return finished;
	}
}
