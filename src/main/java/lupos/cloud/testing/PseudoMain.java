package lupos.cloud.testing;

import org.apache.pig.impl.util.JarManager;

import lupos.cloud.pig.udfs.MapToBag;
import lupos.cloud.pig.udfs.PigLoadUDF;

public class PseudoMain {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("bla: "+ JarGetter.getJar(com.google.protobuf.Message.class));
//		JarManager.findContainingJar();
	}

}