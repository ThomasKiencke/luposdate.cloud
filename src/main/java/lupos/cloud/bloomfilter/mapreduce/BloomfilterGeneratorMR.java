package lupos.cloud.bloomfilter.mapreduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

import lupos.cloud.bloomfilter.BitvectorManager;
import lupos.cloud.hbase.HBaseConnection;
import lupos.cloud.hbase.HBaseDistributionStrategy;

public class BloomfilterGeneratorMR {
	public static Integer MIN_CARD = 25000;
	public static Integer BATCH = 5000;
	public static Integer CACHING = 100;

	/**
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		System.out.println("Starts with b: " + BATCH + " c: " + CACHING);
		HBaseConnection.init();
		ArrayList<BVJobThread> jobList = new ArrayList<BVJobThread>();

		long startTime = System.currentTimeMillis();

		String[] tables =  HBaseDistributionStrategy.getTableInstance()
				.getTableNames();
		
//		String[] tables = { "O_SP", "PO_S"};
//		String[] tables = { "PO_S"};
//		String[] tables = { "P_SO"};
		
		for (String tablename : tables) {
			// String tablename = "P_SO";
			System.out.println("Aktuelle Tabelle: " + tablename);
			BVJobThread curJob = new BVJobThread(tablename);
			jobList.add(curJob);
			curJob.start();
			
		}

		System.out.println("Warte bis alle Jobs abgeschlossen sind ...");
		for (BVJobThread job : jobList) {
			while (!job.isFinished()) {
				sleep(2000);
			}
			System.out.println(job.getTablename() + " ist fertig");
		}

		long stopTime = System.currentTimeMillis();
		System.out.println("Bitvektor Generierung beendet." + " Dauer: "
				+ (stopTime - startTime) / 1000 + "s");
	}

	private static void sleep(int time) {
		try {
			Thread.sleep(time);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}



}
