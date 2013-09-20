package lupos.cloud.testing;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.NavigableMap;

import lupos.cloud.hbase.HBaseConnection;
import lupos.cloud.hbase.bulkLoad.HBaseKVMapper;
import lupos.cloud.pig.JoinInformation;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class BitvectorManager {

	JoinInformation currentSet = null;

	public static void generateBitvector(
			HashMap<String, HashSet<CloudBitvector>> bitvectors)
			throws IOException {
		System.out.println("# bitvectors: " + bitvectors.size());
		for (String var : bitvectors.keySet()) {
			ArrayList<BitSet> bitSetList = new ArrayList<BitSet>();
			if (bitvectors.get(var).size() > 1) {
				for (CloudBitvector bv : bitvectors.get(var)) {
					bitSetList.add(getBitSetFromeHbaseTable(bv.getTablename(), bv.getRow(), bv.getColumnFamily()));
				}
			}

			boolean bitVectorIgnored = false;
			BitSet bitVector = null;

			// Wenn nur ein bitvector vorahnden ist ignroriere diesen
			if (bitSetList.size() == 0) {
				bitVector = new BitSet(HBaseKVMapper.VECTORSIZE);
				bitVector.set(0, HBaseKVMapper.VECTORSIZE);
				bitVectorIgnored = true;
			} else {
				bitVector = mergeBitSet(bitvectors.get(var), var, bitSetList);
			}

			System.out.print("bitvectorsize for " + var + ": ");
			if (bitVectorIgnored || bitVector.cardinality() == bitVector.size()) {
				System.out.print(" vector ignored");
			} else {
				System.out.print(bitVector.cardinality());
			}
			System.out.println();

			Path local = new Path("cloudBloomfilter_" + var.replace("?", ""));
			Path remote = new Path("/tmp/cloudBloomfilter_"
					+ var.replace("?", ""));
			writeByteToDisk(toByteArray(bitVector), local);
			HBaseConnection.getHdfs_fileSystem().copyFromLocalFile(true, true,
					local, remote);
			new File(local.getName()).delete();
		}

	}
	
	private static BitSet getBitSetFromeHbaseTable(String tablename, String row, String cf) throws IOException {
		BitSet bitvector = new BitSet(HBaseKVMapper.VECTORSIZE);
		
		//init scan
		Scan s = new Scan();
		s.setStartRow(Bytes.toBytes(row));
		s.setStopRow(Bytes.toBytes(row + "z"));
		s.setBatch(100000);
		
		// get Result and store it to BitSet
		HTable hTable = new HTable(HBaseConnection.getConfiguration(), tablename);
		ResultScanner scanner = hTable.getScanner(s);
		for (Result res = scanner.next(); res != null; res = scanner.next()) {
			 addResultToBitSet(bitvector, res, cf);
		}
		
		// cleanup
		hTable.close();
		
		return bitvector;
	}


	public static Result getBitvectorHbaseResult(String tablename,
			String columnFamily, String column) throws IOException {
		HBaseConnection.init();
		return HBaseConnection
				.getRowWithColumn(tablename, column, columnFamily);
	}

	public static BitSet addResultToBitSet(BitSet bitvector, Result resultMap, String cf)
			throws IOException {
		NavigableMap<byte[], byte[]> cfResults = resultMap.getFamilyMap(cf
				.getBytes());
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

	public static void writeByteToDisk(byte[] toWrite, Path path)
			throws IOException {
		FileOutputStream fos = new FileOutputStream(path.getName());
		fos.write(toWrite);
		fos.flush();
		fos.close();
	}

	public static BitSet mergeBitSet(HashSet<CloudBitvector> hashSet,
			String var, ArrayList<BitSet> bitSetList) throws IOException {
		System.out.println("\n--- Merge BitSet for " + var + " ----");
		BitSet bitvector = new BitSet(HBaseKVMapper.VECTORSIZE);
		int j = 0;
		ArrayList<CloudBitvector> list = new ArrayList<CloudBitvector>(hashSet);
		for (BitSet bs : bitSetList) {
			System.out.println("Size before " + bs.cardinality() + " for "
					+ list.get(j).getRow() + " in table "
					+ list.get(j).getTablename());
			j++;
		}
		for (int i = 0; i < bitvector.size(); i++) {
			boolean setBit = true;
			for (BitSet bits : bitSetList) {
				if (!bits.get(i)) {
					setBit = false;
				}
			}
			if (setBit) {
				bitvector.set(i);
			}
		}
		System.out.println("----> after: " + bitvector.cardinality() + "\n");
		return bitvector;
	}

	public static BitSet getFullSetBitvector() {
		BitSet bitvector = new BitSet(HBaseKVMapper.VECTORSIZE);
		for (int i = 0; i < HBaseKVMapper.VECTORSIZE; i++) {
			bitvector.set(i);
		}
		return bitvector;
	}

	public static byte[] toByteArray(BitSet bits) {
		byte[] bytes = new byte[bits.length() / 8 + 1];
		for (int i = 0; i < bits.length(); i++) {
			if (bits.get(i)) {
				bytes[bytes.length - i / 8 - 1] |= 1 << (i % 8);
			}
		}
		return bytes;
	}
}