package lupos.cloud.testing.mapreduce;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.NavigableMap;

import java17Dependencies.BitSet;

import lupos.cloud.testing.BitvectorManager;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class MyMapper extends TableMapper<Text, Writable> {

	private Text text = new Text();

	public void map(ImmutableBytesWritable row, Result res, Context context)
			throws IOException, InterruptedException {
		int curColSize = res.getFamilyMap(
				BitvectorManager.bloomfilter1ColumnFamily).size();

		BitvectorContainer val = null;
		if (curColSize < BloomfilterGeneratorMR.BATCH - 1) {
			val = new BitvectorContainer();
			val.setResult(res);
			
		} else {

			BitSet bitvector1 = new BitSet(BitvectorManager.VECTORSIZE);
			BitSet bitvector2 = null;

			if (Bytes.toString(res.getRow()).contains(",")) {
				bitvector1 = new BitSet(BitvectorManager.VECTORSIZE);
				addResultToBitSet(false, bitvector1, null, res);
				val = new BitvectorContainer(bitvector1, null);
			} else {
				bitvector1 = new BitSet(BitvectorManager.VECTORSIZE);
				bitvector2 = new BitSet(BitvectorManager.VECTORSIZE);
				addResultToBitSet(true, bitvector1, bitvector2, res);
				val = new BitvectorContainer(bitvector1, bitvector2);

			}
		}

		text.set(Bytes.toString(res.getRow())); // key

		context.getCounter("MyMapper", "ADD_BITVEKTOR").increment(1);
		context.write(text, val);
	}

	private static void addResultToBitSet(Boolean twoBitvectors,
			BitSet bitvector1, BitSet bitvector2, Result res)
			throws UnsupportedEncodingException {
		byte[] bloomfilterColumn = "bloomfilter".getBytes();

		// Bitvektor 1
		NavigableMap<byte[], byte[]> cfResults = res
				.getFamilyMap(BitvectorManager.bloomfilter1ColumnFamily);
		if (cfResults != null) {
			for (byte[] entry : cfResults.keySet()) {
				// Bloomfilter
				if (!Arrays.equals(entry, bloomfilterColumn)) {
					Integer position = byteArrayToInteger(entry);
					bitvector1.set(position);
				}
			}
		}

		// Bitvektor 2
		if (twoBitvectors) {
			cfResults = res
					.getFamilyMap(BitvectorManager.bloomfilter2ColumnFamily);
			if (cfResults != null) {
				for (byte[] entry : cfResults.keySet()) {
					// Bloomfilter
					if (!Arrays.equals(entry, bloomfilterColumn)) {
						Integer position = byteArrayToInteger(entry);
						bitvector2.set(position);
					}
				}
			}
		}
	}
	

	public static byte[] toByteArray(BitSet bits) {
		return bits.toByteArray();
	}

	private static Integer byteArrayToInteger(byte[] arr) {
		return ByteBuffer.wrap(arr).getInt();
	}
}
