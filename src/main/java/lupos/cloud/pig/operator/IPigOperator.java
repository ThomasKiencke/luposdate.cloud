package lupos.cloud.pig.operator;

import java.util.ArrayList;

import lupos.cloud.pig.BagInformation;

/**
 * Jeder PigOperator implementiert die buildQuery() Methode mit der das
 * PigLatin-Programm erzeugt wird.
 */
public interface IPigOperator {

	/**
	 * erzeugut das PigLatin-Programm.
	 * 
	 * @param intermediateBags
	 *            the intermediate bags
	 * @param debug
	 *            the debug
	 * @param filterOps
	 *            the filter ops
	 * @return the string
	 */
	public String buildQuery(ArrayList<BagInformation> intermediateBags,
			boolean debug, ArrayList<PigFilterOperator> filterOps);
}
