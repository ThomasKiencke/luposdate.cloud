package lupos.cloud.bloomfilter.mapreduce;

import java.io.IOException;

import lupos.cloud.bloomfilter.BitvectorManager;
import lupos.cloud.hbase.HBaseConnection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

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
				null, // mapper output key
				null, // mapper output value
				job);

		TableMapReduceUtil.initTableReducerJob(tablename, // output table
				null, // reducer class
				job);
		job.setNumReduceTasks(0); // at least one, adjust as required
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
