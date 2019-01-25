/*
 * Copyright (c) 2017 Data and Web Science Group, University of Mannheim, Germany (http://dws.informatik.uni-mannheim.de/)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
package de.uni_mannheim.informatik.dws.GMKT.match.data;

import java.util.Collection;

import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.FusibleDataSet;
import de.uni_mannheim.informatik.dws.winter.model.FusibleParallelHashedDataSet;
import de.uni_mannheim.informatik.dws.winter.model.ParallelHashedDataSet;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class DataSetLoader {

	public FusibleDataSet<MatchableTableRow, MatchableTableColumn> loadRowDataSet(Collection<Table> tables) {
		
		FusibleDataSet<MatchableTableRow, MatchableTableColumn> records = new FusibleParallelHashedDataSet<>();
		
		for(Table t : tables) {
			
			MatchableTableColumn[] schema = new MatchableTableColumn[t.getSchema().getSize()];
			
			for(TableColumn c : t.getColumns()) {
				MatchableTableColumn matchableColumn = new MatchableTableColumn(t.getTableId(), c);
				schema[c.getColumnIndex()] = matchableColumn;
				records.addAttribute(matchableColumn);
			}
			
			for(TableRow r : t.getRows()) {
				
				MatchableTableRow row = new MatchableTableRow(r, t.getTableId(), schema);
				records.add(row);
			}
			
		}
		
		
		return records;
	}
	
	// Overload method to rename columns
	public DataSet<MatchableTableRow, MatchableTableColumn> loadDataSet(Table t) {
		
		DataSet<MatchableTableRow, MatchableTableColumn> records = new ParallelHashedDataSet<>();
		
		MatchableTableColumn[] schema = new MatchableTableColumn[t.getSchema().getSize()];
		TableColumn identifierColumn = null;
		
		for(TableColumn c : t.getColumns()) {
			MatchableTableColumn matchableColumn = new MatchableTableColumn(t.getTableId(), c);
			schema[c.getColumnIndex()] = matchableColumn;
			records.addAttribute(matchableColumn);
			
			if(c.getHeader().equals("uri")){
				identifierColumn = c;
			}
			
		}
		
		for(TableRow r : t.getRows()) {
			
			MatchableTableRow row = new MatchableTableRow(r, t.getTableId(), schema);
			
			if(identifierColumn != null){
    			row.updateIdentifierFromColumn(identifierColumn.getColumnIndex());
    		}
			
			records.add(row);
		}
		
		return records;
	}
	
}
