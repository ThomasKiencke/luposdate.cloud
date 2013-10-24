package lupos.cloud.testing;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java17Dependencies.BitSet;

public class BitSetTest {
	static  int VECTORSIZE = 1000000000;

	public static void main(String[] args) throws IOException {
		long start = System.currentTimeMillis();
		ArrayList<BitSet> bitList = new ArrayList<BitSet>();
		BitSet b1 = new BitSet(VECTORSIZE);
		BitSet b2 = new BitSet(VECTORSIZE);
		BitSet b3 = new BitSet(VECTORSIZE);
		BitSet b4 = new BitSet(VECTORSIZE);
		b1.set(1000, 100000);
		bitList.add(b1);
		
		b2.set(10, VECTORSIZE);
		bitList.add(b2);
		
		b3.set(100, 1000000000);
		bitList.add(b3);
		
		b4.set(1000, 1000000000);
		bitList.add(b4);
		
	
		mergeBitSet("bla", bitList);
////		for (int i = 0; i < 100; i++) {
//			System.out.println("b1: " + b1.cardinality());
//			System.out.println("b2: " + b2.cardinality());
////		}
			
		b1.and(b2);;
		System.out.println("davor. " + b2.cardinality());
		byte[] erg = b2.toByteArray();
		
		BitSet ergBack = BitSet.valueOf(erg);
		System.out.println("danach. " + ergBack.cardinality());
		

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
//		for (int i = 0; i < bitvector.size(); i++) {
//			boolean setBit = true;
//			for (BitSet bits : bitSetList) {
//				if (!bits.get(i)) {
//					setBit = false;
//				}
//			}
//			if (setBit) {
//				bitvector.set(i);
//			}
//		}
		System.out.println(" to " + bitSetList.get(0).cardinality() + " <---");
		return bitSetList.get(0);
	}
	
	public static byte[] toByteArray(BitSet bits) {
		return new byte[(bits.length() + 7) / 8];
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
}
