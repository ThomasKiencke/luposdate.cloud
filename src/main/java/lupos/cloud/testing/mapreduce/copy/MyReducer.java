package lupos.cloud.testing.mapreduce.copy;

import java.io.IOException;
import java17Dependencies.BitSet;

import lupos.cloud.testing.BitvectorManager;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;

public class MyReducer extends
		TableReducer<Text, BitvectorContainer, ImmutableBytesWritable> {
	
	public void reduce(Text key, Iterable<BitvectorContainer> values, Context context)
			throws IOException, InterruptedException {
		int i = 0;
		BitSet bitvector1 = null;
		BitSet bitvector2 = null;
		for (BitvectorContainer val : values) {
			if (i == 0) {
				bitvector1 = val.getBv1();
				bitvector2 = val.getBv2();
			}
			if (bitvector2 == null) {
				bitvector1.or(val.getBv1());
			} else {
				bitvector1.or(val.getBv1());
				bitvector2.or(val.getBv2());
			}
		}

		if (bitvector1.cardinality() < BloomfilterGeneratorMR.MIN_CARD) {
			// drop
			context.getCounter("MyReducer", "DROPED_BITVECTORS").increment(1);

			return;
		}
		
		Put put = new Put(Bytes.toBytes(key.toString()));
		put.add(BitvectorManager.bloomfilter1ColumnFamily,
				Bytes.toBytes("bloomfilter"), bitvector1.toByteArray());

		if (bitvector2 != null) {
			put.add(BitvectorManager.bloomfilter2ColumnFamily,
					Bytes.toBytes("bloomfilter"), bitvector2.toByteArray());
		}

		context.getCounter("MyReducer", "GENERATED_BITVECTORS").increment(1);
		context.write(null, put);
	}
}