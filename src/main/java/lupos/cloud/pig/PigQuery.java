package lupos.cloud.pig;

import java.util.ArrayList;

/**
 * In dieser Klassen werden Informationen über das PigQuery abgespeichert z.B.
 * das PigLatin Programm selbst sowie die zu erwartende Variablenliste.
 */
public class PigQuery {

	/** The pig latin. */
	StringBuilder pigLatin = new StringBuilder();

	/** The variable list. */
	ArrayList<String> variableList = new ArrayList<String>();

	/**
	 * Instantiates a new pig query.
	 * 
	 * @param pigLatin
	 *            the pig latin
	 * @param variableList
	 *            the variable list
	 */
	public PigQuery(String pigLatin, ArrayList<String> variableList) {
		super();
		this.pigLatin.append(pigLatin);
		this.variableList = variableList;
	}

	/**
	 * Instantiates a new pig query.
	 */
	public PigQuery() {
	}

	/**
	 * Gets the pig latin.
	 * 
	 * @return the pig latin
	 */
	public String getPigLatin() {
		return pigLatin.toString();
	}

	/**
	 * Append pig latin.
	 * 
	 * @param pigLatin
	 *            the pig latin
	 */
	public void appendPigLatin(String pigLatin) {
		this.pigLatin.append(pigLatin);
	}

	/**
	 * Append pig latin.
	 * 
	 * @param pigLatin
	 *            the pig latin
	 */
	public void appendPigLatin(PigQuery pigLatin) {
		this.pigLatin.append(pigLatin.getPigLatin());
		if (pigLatin.getVariableList() != null && this.variableList.size() == 0) {
			this.variableList = pigLatin.getVariableList();
		}
	}

	/**
	 * Gets the variable list.
	 * 
	 * @return the variable list
	 */
	public ArrayList<String> getVariableList() {
		return variableList;
	}

	/**
	 * Sets the variable list.
	 * 
	 * @param variableList
	 *            the new variable list
	 */
	public void setVariableList(ArrayList<String> variableList) {
		this.variableList = variableList;
	}

}
