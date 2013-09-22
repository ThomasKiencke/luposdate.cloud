package lupos.cloud.testing;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.NavigableMap;

import lupos.cloud.hbase.HBaseConnection;
import lupos.cloud.hbase.bulkLoad.HBaseKVMapper;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class BitSetGetThread implements Runnable {
	BitSet result = null;
	String tablename;
	String row;
	byte[] cf;
	boolean isReady = false;
	
	public BitSetGetThread(String tablename,
			String row, byte[] cf) {
		this.row = row;
		this.cf = cf;
		this.tablename = tablename;
	}
    @Override
    public void run() {
         try {
			result = getBitSetFromeHbaseTable();
		} catch (IOException e) {
			e.printStackTrace();
		}
     this.isReady = true;    
    }
    
	private BitSet getBitSetFromeHbaseTable() throws IOException {
		BitSet bitvector = new BitSet(BitvectorManager.VECTORSIZE);
		HTable hTable = new HTable(HBaseConnection.getConfiguration(),
				tablename);


		Scan s = new Scan();
		s.setStartRow(Bytes.toBytes(row));
		s.setStopRow(Bytes.toBytes(row + "z"));
		s.setBatch(250000);
		s.setCacheBlocks(false);
		s.addFamily(cf);

		ResultScanner scanner = hTable.getScanner(s);
		for (Result res = scanner.next(); res != null; res = scanner.next()) {
			addResultToBitSet(bitvector, res, cf);
		}

		// cleanup
		hTable.close();

		return bitvector;
	}
	
	public boolean isReady() {
		return isReady;
	}
	
	public static BitSet addResultToBitSet(BitSet bitvector, Result resultMap,
			byte[] cf) throws IOException {
		NavigableMap<byte[], byte[]> cfResults = resultMap.getFamilyMap(cf);
		if (cfResults != null) {
			for (byte[] entry : cfResults.keySet()) {
				Integer pos = byteArrayToInteger(entry);
				bitvector.set(pos);
			}
		}
		return bitvector;
	}
	
	private static Integer byteArrayToInteger(byte[] arr) {
		return ByteBuffer.wrap(arr).getInt();
	}
	
	public BitSet getResult() {
		return result;
	}
}
