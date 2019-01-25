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

package de.uni_mannheim.informatik.dws.GMKT.blocking;

import java.util.LinkedList;

import de.uni_mannheim.informatik.dws.GMKT.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.GMKT.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.generators.RecordBlockingKeyGenerator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;

/**
 * 
 * @author Alexander Brinkmann (albrinkm@mail.uni-mannheim.de)
 * 
 * Generates a blocking key based on the first three characters of a provided name column
 * If the value includes a "," the values around the first "," are changed. 
 * Example: "Schiller, Friedrich" --> "Friedrich Schiller"
 * 
 */
public class BlockingKeyByName extends
		RecordBlockingKeyGenerator<MatchableTableRow, MatchableTableColumn> {

	private static final long serialVersionUID = 1L;
	private LinkedList<MatchableTableColumn> labels;
	private int noCharacters;
	
	public BlockingKeyByName(LinkedList<MatchableTableColumn> labels, int noCharacters){
		this.labels = labels;
		this.noCharacters = noCharacters;
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.matching.blocking.generators.BlockingKeyGenerator#generateBlockingKeys(de.uni_mannheim.informatik.wdi.model.Matchable, de.uni_mannheim.informatik.wdi.model.Result, de.uni_mannheim.informatik.wdi.processing.DatasetIterator)
	 */
	@Override
	public void generateBlockingKeys(MatchableTableRow record, Processable<Correspondence<MatchableTableColumn, Matchable>> correspondences,
			DataIterator<Pair<String, MatchableTableRow>> resultCollector) {
				for(MatchableTableColumn column : labels){
					if(record.hasValue(column)){
						String value = record.getValue(column).toString().toLowerCase();
						String [] values = value.split(",");
						if(values.length > 1){
							value = values[1] + " " +  values[0];
						}
						String keyValue = value.substring(0, Math.min(value.length(), noCharacters));
						resultCollector.next(new Pair<>(keyValue, record));
					}
				}
	}
	
	public String toString(){
		return String.format("%s-noCharacters:%s", this.getClass().getSimpleName(), Integer.toString(noCharacters));
	}


}
