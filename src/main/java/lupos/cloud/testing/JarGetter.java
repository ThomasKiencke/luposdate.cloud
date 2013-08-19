package lupos.cloud.testing;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;

public class JarGetter {

	/** Original form TableMapReduceUtil */
	public static String getJar(Class my_class){
		try {
			Class<?> jarFinder = Class
					.forName("org.apache.hadoop.util.JarFinder");
			// hadoop-0.23 has a JarFinder class that will create the jar
			// if it doesn't exist. Note that this is needed to run the
			// mapreduce
			// unit tests post-0.23, because mapreduce v2 requires the relevant
			// jars
			// to be in the mr cluster to do output, split, etc. At unit test
			// time,
			// the hbase jars do not exist, so we need to create some. Note that
			// we
			// can safely fall back to findContainingJars for pre-0.23
			// mapreduce.
			Method m = jarFinder.getMethod("getJar", Class.class);
			return (String) m.invoke(null, my_class);
		} catch (InvocationTargetException ite) {
			// function was properly called, but threw it's own exception
			try {
				throw new IOException(ite.getCause());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (Exception e) {
			// ignore all other exceptions. related to reflection failure
		}
		return findContainingJar(my_class);
	}


	/** Original form TableMapReduceUtil */
	private static String findContainingJar(Class my_class) {
		ClassLoader loader = my_class.getClassLoader();
		String class_file = my_class.getName().replaceAll("\\.", "/")
				+ ".class";
		try {
			for (Enumeration itr = loader.getResources(class_file); itr
					.hasMoreElements();) {
				URL url = (URL) itr.nextElement();
				if ("jar".equals(url.getProtocol())) {
					String toReturn = url.getPath();
					if (toReturn.startsWith("file:")) {
						toReturn = toReturn.substring("file:".length());
					}
					// URLDecoder is a misnamed class, since it actually decodes
					// x-www-form-urlencoded MIME type rather than actual
					// URL encoding (which the file path has). Therefore it
					// would
					// decode +s to ' 's which is incorrect (spaces are actually
					// either unencoded or encoded as "%20"). Replace +s first,
					// so
					// that they are kept sacred during the decoding process.
					toReturn = toReturn.replaceAll("\\+", "%2B");
					toReturn = URLDecoder.decode(toReturn, "UTF-8");
					return toReturn.replaceAll("!.*$", "");
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return null;
	}
}
