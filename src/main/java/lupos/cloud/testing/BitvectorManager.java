package lupos.cloud.testing;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
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
			HashMap<String, ArrayList<CloudBitvector>> bitvectors)
			throws IOException {
		System.out.println("# bitvectors: " + bitvectors.size());
		for (String var : bitvectors.keySet()) {
			ArrayList<BitSet> bitSetList = new ArrayList<BitSet>();
			for (CloudBitvector bv : bitvectors.get(var)) {
				Result res = getBitvectorHbaseResult(bv.getTablename(),
						bv.getColumnFamily(), bv.getRow());
				bitSetList.add(resultToBitSet(res, bv.getColumnFamily()));
			}

			BitSet bitVector = mergeBitSet(bitSetList);

			// write Bitvector to hdfs
			FSDataOutputStream output = HBaseConnection.getHdfs_fileSystem()
					.create(new Path("/tmp/cloudBloomfilter_"
							+ var.replace("?", "")));
			byte[] toAdd = toByteArray(bitVector);
			
			BitSet blab = fromByteArray(toAdd);
			output.write(toByteArray(bitVector));
			System.out.print("Bitvectorsize for " + var + ": ");
			printPos(bitVector);
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
				Integer pos = Integer.parseInt(new String(entry));
				bitvector.set(pos);
			}
		}
		return bitvector;
	}

	public static BitSet mergeBitSet(ArrayList<BitSet> bitSetList)
			throws IOException {
		BitSet bitvector = new BitSet(HBaseKVMapper.VECTORSIZE);
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
		return bitvector;
	}

	public static void printPos(BitSet bitvector) {
		int num = 0;
		for (int i = 0; i < bitvector.length(); i++) {
			if (bitvector.get(i)) {
				num++;
//				System.out.println(i);
			}
		}
		System.out.println("Anzahl Set Bits: "+ num);
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