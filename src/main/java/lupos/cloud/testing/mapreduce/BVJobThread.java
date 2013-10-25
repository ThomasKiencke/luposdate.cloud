package lupos.cloud.testing.mapreduce;

import java.io.IOException;

import lupos.cloud.hbase.HBaseConnection;
import lupos.cloud.testing.BitvectorManager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.hadoopbackport.TotalOrderPartitioner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BVJobThread extends Thread {
	private String tablename;
	private Job job;
	private boolean finished = false;

	public BVJobThread(String tablename) throws IOException {
		this.tablename = tablename;
		createJob();
	}

	private void createJob() throws IOException {
		Configuration config = HBaseConnection.getConfiguration();
		 job = new Job(config, "MR_BV_ " + tablename);

		Scan scan = new Scan();
		scan.setCaching(BloomfilterGeneratorMR.CACHING);
		scan.setCacheBlocks(false); // don't set to true for MR jobs
		scan.setBatch(BloomfilterGeneratorMR.BATCH);
		scan.addFamily(BitvectorManager.bloomfilter1ColumnFamily);
		scan.addFamily(BitvectorManager.bloomfilter2ColumnFamily);

		TableMapReduceUtil.initTableMapperJob(tablename, // input table
				scan, // Scan instance to control CF and attribute selection
				MyMapper.class, // mapper class
				Text.class, // mapper output key
				BitvectorContainer.class, // mapper output value
				job);
		job.setReducerClass(MyReducer.class); // reducer class
		job.setNumReduceTasks(1); // at least one, adjust as required

		String working_file = "/tmp/bitvectorGen_" + tablename;
		HBaseConnection.getHdfs_fileSystem().delete(new Path(working_file),
				true);
		FileOutputFormat.setOutputPath(job, new Path(working_file));

	}

	/**
	 * Lädt eine Tabelle per Map Reduce Bulkload.
	 * 
	 * @param tablename
	 *            the tablename
	 */

	public void run() {
		try {
			this.job.waitForCompletion(true);
			this.finished = true;

		} catch (TableNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public Job getJob() {
		return job;
	}

	public boolean isFinished() {
		return finished;
	}
	
	public String getTablename() {
		return tablename;
	}
}
