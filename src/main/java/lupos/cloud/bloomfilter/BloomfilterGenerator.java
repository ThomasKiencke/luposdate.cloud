package lupos.cloud.bloomfilter;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.NavigableMap;

import java17Dependencies.BitSet;
import lupos.cloud.hbase.HBaseConnection;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Mit Hilfe dieser Klasse wird die Byte-Bitvektorgeneierung gestartet. Dabei
 * wird die Verbindung direkt über die HBase API hergestellt und jeder Rowkey
 * durchgegangen.
 * 
 * @deprecated Kommunikation per HBase-Api ist zu langsam. Besser BloomfilterGeneratorMR verwenden.
 */
@Deprecated
public class BloomfilterGenerator {

	/** The min card. */
	private static Integer MIN_CARD = 100;

	/**
	 * The main method.
	 * 
	 * @param args
	 *            the arguments
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.out.println("java -jar programm <batchSize> <CachingSize>");
			System.exit(0);
		}

		int batchSize = Integer.parseInt(args[0]);
		int cachingSize = Integer.parseInt(args[1]);

		HBaseConnection.init();
		int bitvectorCount = 0;
		long startTime = System.currentTimeMillis();
		long checkedNumber = 0;

		// for (String tablename : HBaseDistributionStrategy.getTableInstance()
		// .getTableNames()) {
		String tablename = "PO_S";
		System.out.println("Aktuelle Tabelle: " + tablename);
		HTable hTable = new HTable(HBaseConnection.getConfiguration(),
				tablename);

		Scan s = new Scan();
		s.setBatch(batchSize);
		s.setCaching(cachingSize);
		s.setCacheBlocks(true);

		s.addFamily(BitvectorManager.bloomfilter1ColumnFamily);
		s.addFamily(BitvectorManager.bloomfilter2ColumnFamily);

		ResultScanner scanner = hTable.getScanner(s);

		byte[] lastRowkey = null;
		BitSet bitvector1 = new BitSet(BitvectorManager.VECTORSIZE);
		BitSet bitvector2 = new BitSet(BitvectorManager.VECTORSIZE);
		boolean reset = true;
		byte[] curBitvectorName = null;
		for (Result res = scanner.next(); res != null; res = scanner.next()) {
			// Ausgabe der momentanen Position
			if (checkedNumber % 1000000 == 0) {
				System.out.println(checkedNumber + " Rows checked");
			}
			checkedNumber++;

			// Wenn nur sehr wenige Elemente in der Reihe vorhanden sind,
			// ueberspringe diese
			int curColSize = res.getFamilyMap(
					BitvectorManager.bloomfilter1ColumnFamily).size();

			if (curColSize < batchSize
					&& !Arrays.equals(lastRowkey, res.getRow())) {
				lastRowkey = res.getRow();
				continue;
			}

			// Speichere Bitvektoren
			if (lastRowkey != null && !Arrays.equals(lastRowkey, res.getRow())) {
				if (bitvector1.cardinality() >= MIN_CARD) {
					// store bitvectors
					storeBitvectorToHBase(tablename, curBitvectorName,
							bitvector1, bitvector2, hTable);
					bitvectorCount++;
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

		// letzten Bitvektor speichern
		if (lastRowkey != null) {
			if (bitvector1.cardinality() >= MIN_CARD) {
				storeBitvectorToHBase(tablename, curBitvectorName, bitvector1,
						bitvector2, hTable);
			}
		}

		// cleanup
		scanner.close();
		hTable.close();
		// } // close
		long stopTime = System.currentTimeMillis();
		System.out
				.println("Bitvektor Generierung beendet. Anzahl der erzeugten Bitvektoren: "
						+ bitvectorCount
						+ " Dauer: "
						+ (stopTime - startTime)
						/ 1000 + "s");
	}


	/**
	 * Adds the result to bit set.
	 * 
	 * @param twoBitvectors
	 *            the two bitvectors
	 * @param bitvector1
	 *            the bitvector1
	 * @param bitvector2
	 *            the bitvector2
	 * @param res
	 *            the res
	 * @throws UnsupportedEncodingException
	 *             the unsupported encoding exception
	 */
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

	/**
	 * Byte array to integer.
	 * 
	 * @param arr
	 *            the arr
	 * @return the integer
	 */
	private static Integer byteArrayToInteger(byte[] arr) {
		return ByteBuffer.wrap(arr).getInt();
	}

	/**
	 * Store bitvector to h base.
	 * 
	 * @param tablename
	 *            the tablename
	 * @param rowkey
	 *            the rowkey
	 * @param bitvector1
	 *            the bitvector1
	 * @param bitvector2
	 *            the bitvector2
	 * @param table
	 *            the table
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	private static void storeBitvectorToHBase(String tablename, byte[] rowkey,
			BitSet bitvector1, BitSet bitvector2, HTable table)
			throws IOException {
		// HTable table = new HTable(HBaseConnection.getConfiguration(),
		// tablename);
		Put row = new Put(rowkey);
		row.add(BitvectorManager.bloomfilter1ColumnFamily,
				Bytes.toBytes("bloomfilter"), toByteArray(bitvector1));
		if (bitvector2 != null) {
			row.add(BitvectorManager.bloomfilter2ColumnFamily,
					Bytes.toBytes("bloomfilter"), toByteArray(bitvector2));
		}
		table.put(row);
		// table.close();
		System.out.println("Tabelle : " + tablename + " RowKey: "
				+ Bytes.toString(rowkey) + " Bitvector-Size: "
				+ bitvector1.cardinality());
	}

	/**
	 * To byte array.
	 * 
	 * @param bits
	 *            the bits
	 * @return the byte[]
	 */
	public static byte[] toByteArray(BitSet bits) {
		return bits.toByteArray();
	}

}
