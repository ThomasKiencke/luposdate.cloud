package lupos.cloud.testing;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.velocity.runtime.directive.Literal;

import lupos.cloud.storage.util.HBaseConnection;
import lupos.datastructures.items.Triple;

public class Test {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		// // HBaseConnection.deleteTable("test");
		// HBaseConnection.createTable("S_PO", "VALUE");
		// HBaseConnection.addRow("S_PO", "S", "P O");
		// HBaseConnection.addRow("S_PO", "S2", "P2 O2");
		// // HBaseConnection.listAllTables();
		// HBaseConnection.printTable("P_SO");
//		HBaseConnection.getRow("S_PO",
//				"<http://localhost/publications/journals/Journal1/1940>");
		
		HBaseConnection.createTable("testTable", "VALUE");
		HBaseConnection.addRow("testTable", "a", "b1");
		HBaseConnection.addRow("testTable", "a", "b2");

		// String table = "S_PO";
		// String row_key_string = table.substring(0, table.indexOf("_"));
		// String column_name_string = table.substring(table.indexOf("_") + 1,
		// table.length());
		// System.out.println(row_key_string + "    " + column_name_string);
		//
		// String elements = column_name_string;
		// System.out.println(elements);

	}

}
