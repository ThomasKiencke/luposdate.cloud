package lupos.cloud.testing;

import java.io.IOException;
import java.util.BitSet;
import java.util.NavigableMap;

import lupos.cloud.hbase.HBaseConnection;
import lupos.cloud.hbase.HBaseDistributionStrategy;
import lupos.cloud.hbase.HexaDistributionTableStrategy;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class BloomfilterGenerator {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		HBaseConnection.init();

		for (String tablename : HBaseDistributionStrategy.getTableInstance()
				.getTableNames()) {
			HTable hTable = new HTable(HBaseConnection.getConfiguration(),
					tablename);

			Scan s = new Scan();
			s.setBatch(50000);
			s.setCacheBlocks(false);
			s.addFamily(HexaDistributionTableStrategy.getTableInstance()
					.getColumnFamilyName().getBytes());

			ResultScanner scanner = hTable.getScanner(s);
			byte[] lastRowkey = null;
			BitSet bitvector1 = null;
			BitSet bitvector2 = null;
			for (Result res = scanner.next(); res != null; res = scanner.next()) {
				// TODO: prüfe ob wirklich rowkey in getRow drin ist 
				if (lastRowkey != res.getRow()) {
					storeBitvectorToHBase(lastRowkey, bitvector1, bitvector2);
					// reset
					bitvector1 = null;
					bitvector2 = null;
					}
				
				if (bitvector1 == null) {
					String curKey = res.getRow().toString();
					if (curKey.contains(",")) {
						bitvector1 = new BitSet(BitvectorManager.VECTORSIZE);
						bitvector2 = null;
					} else {
						bitvector1 = new BitSet(BitvectorManager.VECTORSIZE);
						bitvector2 = new BitSet(BitvectorManager.VECTORSIZE);
					}
				} 
			
				addResultToBitSet(bitvector1, bitvector2, res);
				lastRowkey = res.getRow();
			}
			// cleanup
			hTable.close();
		}
	}

	private static void addResultToBitSet(BitSet bitvector1, BitSet bitvector2,
			Result res) {
		NavigableMap<byte[], byte[]> cfResults = res
				.getFamilyMap(HexaDistributionTableStrategy.getTableInstance()
						.getColumnFamilyName().getBytes());
		if (cfResults != null) {
			for (byte[] entry : cfResults.keySet()) {
				String toSplit = entry.toString();
				String elem1 = null;
				String elem2 = null;
				if (toSplit.contains(",")) {
					elem1 = toSplit.substring(0, toSplit.indexOf(","));
					elem2 = toSplit.substring(toSplit.indexOf(",") + 1,
							toSplit.length());
				} else {
					elem1 = toSplit.substring(0, toSplit.length());
				}
				// Bloomfilter
				if (!(elem1 == null)) {
					Integer position = BitvectorManager.hash(elem1.getBytes());
					byte[] cf = BitvectorManager.bloomfilter1ColumnFamily;
//					byte[]
					KeyValue kv2 = new KeyValue(ibKey.get(),
							BitvectorManager.bloomfilter1ColumnFamily,
							IntegerToByteArray(4, position), "".getBytes());
					context.write(ibKey, kv2);
				}

				if (!(elem2 == null)) {
					Integer position = BitvectorManager.hash(elem2.getBytes());
					KeyValue kv2 = new KeyValue(ibKey.get(),
							BitvectorManager.bloomfilter2ColumnFamily,
							IntegerToByteArray(4, position), "".getBytes());
					context.write(ibKey, kv2);
				}

				Integer pos = byteArrayToInteger(entry);
				bitvector.set(pos);
			}
		}
	}

	private static void storeBitvectorToHBase(byte[] lastRowkey,
			BitSet bitvector) {
		// TODO Auto-generated method stub

	}

}
