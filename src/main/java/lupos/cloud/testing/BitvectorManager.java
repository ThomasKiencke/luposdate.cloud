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
import org.apache.hadoop.hbase.client.Result;

public class BitvectorManager {

	JoinInformation currentSet = null;

	public static void generateBitvector(
			HashMap<String, HashSet<CloudBitvector>> bitvectors)
			throws IOException {
		System.out.println("# bitvectors: " + bitvectors.size());
		for (String var : bitvectors.keySet()) {
			ArrayList<BitSet> bitSetList = new ArrayList<BitSet>();
			for (CloudBitvector bv : bitvectors.get(var)) {
				Result res = getBitvectorHbaseResult(bv.getTablename(),
						bv.getColumnFamily(), bv.getRow());
				bitSetList.add(resultToBitSet(res, bv.getColumnFamily()));
			}

			boolean bitVectorIgnored = false;
			BitSet bitVector = null;

			// Wenn nur ein bitvector vorahnden ist ignroriere diesen
			if (bitSetList.size() == 1) {
				bitVector = new BitSet(HBaseKVMapper.VECTORSIZE);
				bitVector.set(0, bitVector.length());
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

	public static BitSet fromByteArray(byte[] bytes) {
		BitSet bits = new BitSet();
		for (int i = 0; i < bytes.length * 8; i++) {
			if ((bytes[bytes.length - i / 8 - 1] & (1 << (i % 8))) > 0) {
				bits.set(i);
			}
		}
		return bits;
	}

	public static Result getBitvectorHbaseResult(String tablename,
			String columnFamily, String column) throws IOException {
		HBaseConnection.init();
		return HBaseConnection
				.getRowWithColumn(tablename, column, columnFamily);
	}

	public static BitSet resultToBitSet(Result resultMap, String cf)
			throws IOException {
		NavigableMap<byte[], byte[]> cfResults = resultMap.getFamilyMap(cf
				.getBytes());
		BitSet bitvector = new BitSet(HBaseKVMapper.VECTORSIZE);
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

	public static BitSet mergeBitSet(HashSet<CloudBitvector> hashSet, String var, ArrayList<BitSet> bitSetList)
			throws IOException {
		System.out.println("\n--- Merge BitSet for " + var + " ----");
		BitSet bitvector = new BitSet(HBaseKVMapper.VECTORSIZE);
		int j = 0;
		ArrayList<CloudBitvector> list = new ArrayList<CloudBitvector>(hashSet);
		for (BitSet bs : bitSetList) {
			System.out.println("Size before " + bs.cardinality() + " for " + list.get(j).getRow() + " in table " + list.get(j).getTablename());
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