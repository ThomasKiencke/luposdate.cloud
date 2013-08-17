package lupos.cloud.pig;

import java.util.ArrayList;

public class PigQuery {
	
	StringBuilder pigLatin = new StringBuilder();
	ArrayList<String> variableList = new ArrayList<String>();
	
	
	public PigQuery(String pigLatin, ArrayList<String> variableList) {
		super();
		this.pigLatin.append(pigLatin);
		this.variableList = variableList;
	}
	
	public PigQuery() {
	}
	public String getPigLatin() {
		return pigLatin.toString();
	}
	public void appendPigLatin(String pigLatin) {
		this.pigLatin.append(pigLatin);
	}
	
	public void appendPigLatin(PigQuery pigLatin) {
		this.pigLatin.append(pigLatin.getPigLatin());
		if (pigLatin.getVariableList() != null && this.variableList.size() == 0) {
			this.variableList = pigLatin.getVariableList();
		}
	}
	
	public ArrayList<String> getVariableList() {
		return variableList;
	}
	public void setVariableList(ArrayList<String> variableList) {
		this.variableList = variableList;
	}
	
	

}
