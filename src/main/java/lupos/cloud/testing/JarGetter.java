package lupos.cloud.testing;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.hadoop.hbase.mapreduce.hadoopbackport.JarFinder;

public class JarGetter {

	  @SuppressWarnings("unused")
	public static String getJar(Class<?> my_class) {
		    String ret = null;
		    String hadoopJarFinder = "org.apache.hadoop.util.JarFinder";
		    Class<?> jarFinder = null;
		    try {
		      jarFinder = Class.forName(hadoopJarFinder);
		      Method getJar = jarFinder.getMethod("getJar", Class.class);
		      ret = (String) getJar.invoke(null, my_class);
		    } catch (ClassNotFoundException e) {
		      ret = JarFinder.getJar(my_class);
		    } catch (InvocationTargetException e) {
		      // function was properly called, but threw it's own exception. Unwrap it
		      // and pass it on.
		      throw new RuntimeException(e.getCause());
		    } catch (Exception e) {
		      // toss all other exceptions, related to reflection failure
		      throw new RuntimeException("getJar invocation failed.", e);
		    }

		    return ret;
		  }	
	
}
