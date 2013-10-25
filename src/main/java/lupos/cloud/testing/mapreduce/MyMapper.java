package lupos.cloud.testing.mapreduce;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.NavigableMap;

import java17Dependencies.BitSet;
import lupos.cloud.testing.BitvectorManager;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

public class MyMapper extends TableMapper<ImmutableBytesWritable, Put> {

	byte[] lastRowkey = null;
	BitSet bitvector1 = new BitSet(BitvectorManager.VECTORSIZE);
	BitSet bitvector2 = new BitSet(BitvectorManager.VECTORSIZE);
	byte[] curBitvectorName = null;
	boolean reset = true;

	public void map(ImmutableBytesWritable row, Result res, Context context)
			throws IOException, InterruptedException {
		// Wenn nur sehr wenige Elemente in der Reihe vorhanden sind,
		// ueberspringe diese
		int curColSize = res.getFamilyMap(
				BitvectorManager.bloomfilter1ColumnFamily).size();

		if (curColSize < BloomfilterGeneratorMR.BATCH - 1
				&& !Arrays.equals(lastRowkey, res.getRow())) {
			lastRowkey = res.getRow();
			context.getCounter("MyMapper", "SKIP_ROW").increment(1);
			return;
		}

		// Speichere Bitvektoren
		if (lastRowkey != null && !Arrays.equals(lastRowkey, res.getRow())) {
			if (bitvector1.cardinality() >= BloomfilterGeneratorMR.MIN_CARD) {
				// store bitvectors
				storeBitvectorToHBase(curBitvectorName, bitvector1, bitvector2,
						res, context);
			}
			// reset
			reset = true;
		}

		String curKey = Bytes.toString(res.getRow());
		if (reset) {
			curBitvectorName = res.getRow();
			bitvector1.clear();
			bitvector2.clear();
			reset = false;
		}

		if (curKey.contains(",")) {
			addResultToBitSet(false, bitvector1, bitvector2, res);
		} else {
			addResultToBitSet(true, bitvector1, bitvector2, res);
		}

		lastRowkey = res.getRow();
	}

	private void storeBitvectorToHBase(byte[] curBitvectorName2,
			BitSet bitvector1, BitSet bitvector2, Result res, Context context) throws IOException, InterruptedException {
		Put row = new Put(res.getRow());
		row.add(BitvectorManager.bloomfilter1ColumnFamily,
				Bytes.toBytes("bloomfilter"), toByteArray(bitvector1));
		if (bitvector2.cardinality() > 0) {
			row.add(BitvectorManager.bloomfilter2ColumnFamily,
					Bytes.toBytes("bloomfilter"), toByteArray(bitvector2));
		}
		context.getCounter("MyMapper", "ADD_BYTE_BITVEKTOR").increment(1);
		context.write(null,  row);
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
