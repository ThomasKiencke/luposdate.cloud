package lupos.cloud.bloomfilter.mapreduce;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.NavigableMap;

import java17Dependencies.BitSet;
import lupos.cloud.bloomfilter.BitvectorManager;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.xerial.snappy.Snappy;

/**
 * Innerhalb dieser Mapper-Klasse befindet sich die eigentliche Logik zum
 * erzeugen der Byte-Bitvektoren. Für jede in HBase wird die map()-Funktion
 * aufgerufen. Dort wird jeweils überprüft ob die Anzahl der Spalten-Elemente
 * größer als 25000 ist. Da jedoch nicht die gesamte Reihe auf einmal geladen
 * werden kann müssen die verschiedenen "batches" addiert werden und danach
 * erfolgt dann die Üebrprüfung ob die Reihe die Bedingung erfüllt. Ist die
 * Anzahl der Reihen kleiner als die maximale Anzahl Batch-Anzahl wird die Reihe
 * sofort verworfen.
 */
public class MyMapper extends TableMapper<ImmutableBytesWritable, Put> {

	/** Rowkey des letzten Durchlaufs. */
	byte[] lastRowkey = null;

	/** Bitvektor des ersten Elements. */
	BitSet bitvector1 = new BitSet(BitvectorManager.VECTORSIZE);

	/** Bitvektor des zweiten Elements. */
	BitSet bitvector2 = new BitSet(BitvectorManager.VECTORSIZE);

	/** Rowkey, für den der BV gespeichert werden oll */
	byte[] curBitvectorName = null;

	/** Bei setzetn dieser Variable werden die Bitvektore resetet. */
	boolean reset = true;

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
	 * org.apache.hadoop.mapreduce.Mapper.Context)
	 */
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
				NavigableMap<byte[], byte[]> cfResults = res
						.getFamilyMap(BitvectorManager.bloomfilter1ColumnFamily);
				// store bitvectors
				storeBitvectorToHBase(curBitvectorName, bitvector1, bitvector2,
						context);
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
			addResultToBitSet(false, bitvector1, bitvector2, res, context);
		} else {
			addResultToBitSet(true, bitvector1, bitvector2, res, context);
		}

		lastRowkey = res.getRow();
	}

	/**
	 * Diese Methode wird am Ende des Jobs aufgerufen. In dem Fall wird der
	 * Bitvektor der letzten Relevanten Reihe übertragen.
	 * 
	 * @param context
	 *            the context
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 * @throws InterruptedException
	 *             the interrupted exception
	 */
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		if (curBitvectorName != null) {
			context.getCounter("MyMapper", "ADD_BYTE_BITVEKTOR").increment(1);
			// finally
			storeBitvectorToHBase(curBitvectorName, bitvector1, bitvector2,
					context);
		}
	}

	/**
	 * Speichert den Bitvektor (komprimiert mit Snappy) in HBase ab.
	 * 
	 * @param curBitvectorName2
	 *            the cur bitvector name2
	 * @param bitvector1
	 *            the bitvector1
	 * @param bitvector2
	 *            the bitvector2
	 * @param context
	 *            the context
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 * @throws InterruptedException
	 *             the interrupted exception
	 */
	private void storeBitvectorToHBase(byte[] curBitvectorName2,
			BitSet bitvector1, BitSet bitvector2, Context context)
			throws IOException, InterruptedException {
		Put row = new Put(curBitvectorName2);

		row.setWriteToWAL(false);

		byte[] compressedBitvector1 = Snappy.compress(toByteArray(bitvector1));
		row.add(BitvectorManager.bloomfilter1ColumnFamily,
				Bytes.toBytes("bloomfilter"), compressedBitvector1);

		if (bitvector2.cardinality() > 0) {
			byte[] compressedBitvector2 = Snappy
					.compress(toByteArray(bitvector2));
			row.add(BitvectorManager.bloomfilter2ColumnFamily,
					Bytes.toBytes("bloomfilter"), compressedBitvector2);
		}
		context.getCounter("MyMapper", "ADD_BYTE_BITVEKTOR").increment(1);
		ImmutableBytesWritable key = new ImmutableBytesWritable(
				curBitvectorName2);
		context.write(key, row);
	}

	/**
	 * Setzt die Indizes der jeweiligen Zeile im aktuellen Bitvektor.
	 * 
	 * @param twoBitvectors
	 *            the two bitvectors
	 * @param bitvector1
	 *            the bitvector1
	 * @param bitvector2
	 *            the bitvector2
	 * @param res
	 *            the res
	 * @param context
	 *            the context
	 * @throws UnsupportedEncodingException
	 *             the unsupported encoding exception
	 */
	private static void addResultToBitSet(Boolean twoBitvectors,
			BitSet bitvector1, BitSet bitvector2, Result res, Context context)
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
				} else {
					context.getCounter("MyMapper", "BITVECTOR_EXIST_ALREADY")
							.increment(1);
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
					} else {
						context.getCounter("MyMapper",
								"BITVECTOR_EXIST_ALREADY").increment(1);
					}
				}
			}
		}
	}

	/**
	 * BitSet -> ByteArray
	 * 
	 * @param bits
	 *            the bits
	 * @return the byte[]
	 */
	public static byte[] toByteArray(BitSet bits) {
		return bits.toByteArray();
	}

	/**
	 * ByteArray -> BitSet.
	 * 
	 * @param arr
	 *            the arr
	 * @return the integer
	 */
	private static Integer byteArrayToInteger(byte[] arr) {
		return ByteBuffer.wrap(arr).getInt();
	}
}
