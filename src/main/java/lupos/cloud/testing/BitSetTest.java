package lupos.cloud.testing;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.commons.beanutils.converters.LongArrayConverter;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;

import java17Dependencies.BitSet;

public class BitSetTest {
	static int VECTORSIZE = 100;

	public static void main(String[] args) throws IOException {
		long start = System.currentTimeMillis();
		ArrayList<BitSet> bitList = new ArrayList<BitSet>();

		Random gen = new Random();
		// for (int i = 0; i < 1; i++) {
		// BitSet b = new BitSet(VECTORSIZE);
		// int ran = gen.nextInt(VECTORSIZE);
		// b.set(ran,VECTORSIZE);
		// bitList.add(b);
		// System.out.println(b.toString());
		//
		// }
		BitSet b1 = new BitSet(VECTORSIZE);
		int ran = gen.nextInt(VECTORSIZE);
		b1.set(ran, VECTORSIZE);
		BitSet b2 = new BitSet(VECTORSIZE);
		ran = gen.nextInt(VECTORSIZE);
		b2.set(ran, VECTORSIZE);

		String s1 = String.valueOf(b1.toByteArray());
		String s2 = b2.toString();
		String concat = s1 + "x" + s2;

		BitSet b3 = new BitSet(10);
//		b3.set(1);
		System.out.println("asd:" + b3.length());
//		System.out.println("before: " + b1.cardinality());
//
//		BitSet after = BitSet.valueOf(s1.getBytes());
//
//		System.out.println("after: " + after.cardinality());

		// mergeBitSet("bla", bitList);

		// b1.and(b2);;
		// System.out.println("davor. " + b2.length());
		// byte[] erg = b2.toByteArray();
		//
		// BitSet ergBack = fromByteArray(erg);
		// System.out.println("danach. " + ergBack.length());
		// System.out.println("Iss: " +Arrays.equals(erg,
		// ergBack.toByteArray()));
		//

		long stop = System.currentTimeMillis();
		System.out.println("In " + ((stop - start) / (double) 1000)
				+ "s ausgeführt!");

	}

	public static BitSet mergeBitSet(String var, List<BitSet> bitSetList)
			throws IOException {
		if (bitSetList.size() == 1) {
			return bitSetList.get(0);
		}
		System.out.print("\n---> " + var + " is merged (and) from ");
		int j = 0;
		for (BitSet bs : bitSetList) {
			if (j > 0) {
				System.out.print(", ");
			}
			int card = bs.cardinality();
			System.out.print(card);
			j++;
		}

		for (int i = 1; i < bitSetList.size(); i++) {
			bitSetList.get(0).and(bitSetList.get(i));
		}
		// for (int i = 0; i < bitvector.size(); i++) {
		// boolean setBit = true;
		// for (BitSet bits : bitSetList) {
		// if (!bits.get(i)) {
		// setBit = false;
		// }
		// }
		// if (setBit) {
		// bitvector.set(i);
		// }
		// }
		System.out.println(" to " + bitSetList.get(0).cardinality() + " <---");
		return bitSetList.get(0);
	}

	public static byte[] toByteArray(BitSet bits) {
		return new byte[(bits.length() + 7) / 8];
	}

	public static BitSet fromByteArray(byte[] bytes) {
		return BitSet.valueOf(bytes);
		// BitSet bits = new BitSet();
		// for (int i = 0; i < bytes.length * 8; i++) {
		// if ((bytes[bytes.length - i / 8 - 1] & (1 << (i % 8))) > 0) {
		// bits.set(i);
		// }
		// }
		// return bits;
	}
}
