package de.uni_mannheim.informatik.dws.GMKT.match.comparators;

import de.uni_mannheim.informatik.dws.GMKT.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.GMKT.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.matching.rules.Comparator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;

public abstract class RowKBComparator implements Comparator<MatchableTableRow, MatchableTableColumn>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private MatchableTableColumn kbSchemaElement;
	
	public void setFirstSchemaElement(MatchableTableColumn kbSchemaElement){
		this.kbSchemaElement = kbSchemaElement;
	}
	
	public MatchableTableColumn getFirstSchemaElement(MatchableTableRow record) {
		return this.kbSchemaElement;
	}
	

	public String getName(Correspondence<MatchableTableColumn, Matchable> schemaCorrespondence) 
	{	
		return String.format("%s~%s", this.getClass().getSimpleName().replace("RowComparator", ""), this.kbSchemaElement.getHeader());
	}

}
