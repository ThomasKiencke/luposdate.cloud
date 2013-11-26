package lupos.cloud.bloomfilter.mapreduce;

import java.io.IOException;

import lupos.cloud.bloomfilter.BitvectorManager;
import lupos.cloud.hbase.HBaseConnection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;

/**
 * Für jede Tabelle wird ein eigener Thread erzeugt der für die
 * Byte-Bitvektorgeneierung zuständig ist.
 */
public class BVJobThread extends Thread {

	/** Tabellenname. */
	private String tablename;

	/** MapReduce-Job Referenz. */
	private Job job;

	/** Status des Jobs. */
	private boolean finished = false;

	/**
	 * Instantiates a new bV job thread.
	 * 
	 * @param tablename
	 *            the tablename
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public BVJobThread(String tablename) throws IOException {
		this.tablename = tablename;
		createJob();
	}

	/**
	 * Creates the job.
	 * 
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	private void createJob() throws IOException {
		Configuration config = HBaseConnection.getConfiguration();
		job = new Job(config, "MR_BV_ " + tablename);

		Scan scan = new Scan();
		int caching = BloomfilterGeneratorMR.CACHING;
		if (tablename.equals("P_SO")) {
			caching = 2;
		}
		scan.setCaching(caching);
		scan.setCacheBlocks(false); // don't set to true for MR jobs
		scan.setBatch(BloomfilterGeneratorMR.BATCH);
		scan.setFilter(new QualifierFilter(CompareOp.NOT_EQUAL,
				new BinaryComparator(Bytes.toBytes("bloomfilter"))));
		scan.addFamily(BitvectorManager.bloomfilter1ColumnFamily);
		scan.addFamily(BitvectorManager.bloomfilter2ColumnFamily);
		scan.setMaxVersions(1);

		TableMapReduceUtil.initTableMapperJob(tablename, // input table
				scan, // Scan instance to control CF and attribute selection
				MyMapper.class, // mapper class
				null, // mapper output key
				null, // mapper output value
				job);

		TableMapReduceUtil.initTableReducerJob(tablename, // output table
				null, // reducer class
				job);
		job.setNumReduceTasks(0); // kein Reduce Task notwendig
	}

	/**
	 * Thread run() Method.
	 * 
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

	/**
	 * Gets the tablename.
	 * 
	 * @return the tablename
	 */
	public String getTablename() {
		return tablename;
	}
}
