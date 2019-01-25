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
package de.uni_mannheim.informatik.dws.GMKT.match.comparators;

import java.time.LocalDateTime;

import de.uni_mannheim.informatik.dws.GMKT.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.GMKT.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.matching.rules.Comparator;
import de.uni_mannheim.informatik.dws.winter.matching.rules.ComparatorLogger;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.TypeConverter;
import de.uni_mannheim.informatik.dws.winter.similarity.string.LevenshteinSimilarity;

/**
 * {@link Comparator} for {@link Record}s based on the values, and their
 * {@link LevenshteinSimilarity} similarity.
 * 
 * @author Alexander Brinkmann (albrinkm@mail.uni-mannheim.de)
 * 
 */
public class RowComparatorDateLevenshtein extends StringComparator {

	public RowComparatorDateLevenshtein(double threshold, boolean squared) {
		this.setSquared(squared);
		this.setThreshold(threshold);
	}
	

	private static final long serialVersionUID = 1L;
	private LevenshteinSimilarity sim = new LevenshteinSimilarity();
	private ComparatorLogger comparisonLog;

	private TypeConverter tc = new TypeConverter();

	@Override
	public double compare(MatchableTableRow record1, MatchableTableRow record2,
			Correspondence<MatchableTableColumn, Matchable> schemaCorrespondence) {

		if (this.comparisonLog != null) {
			this.comparisonLog.setComparatorName(getClass().getName());
		}

		String s1 = null;
		String s2 = null;

		LocalDateTime date1 = null;
		LocalDateTime date2 = null;

		String prepS1 = null;
		String prepS2 = null;

		if (record1.hasValue(schemaCorrespondence.getFirstRecord())) {
			s1 = record1.getValue(schemaCorrespondence.getFirstRecord()).toString();
			date1 = (LocalDateTime) tc.typeValue(s1, DataType.date, null);

			prepS1 = date1.toString();

			if (this.comparisonLog != null) {
				this.comparisonLog.setRecord1Value(s1);
				this.comparisonLog.setRecord1PreprocessedValue(prepS1);
			}

		} else {
			if (this.comparisonLog != null) {
				this.comparisonLog.setRecord1Value("");
				this.comparisonLog.setRecord1PreprocessedValue("");
			}
		}

		if (record2.hasValue(schemaCorrespondence.getSecondRecord())) {
			s2 = record2.getValue(schemaCorrespondence.getSecondRecord()).toString();
			date2 = (LocalDateTime) tc.typeValue(s2, DataType.date, null);

			prepS2 = date2.toString();

			if (this.comparisonLog != null) {
				this.comparisonLog.setRecord2Value(s2);
				this.comparisonLog.setRecord2PreprocessedValue(prepS2);
			}

		} else {
			if (this.comparisonLog != null) {
				this.comparisonLog.setRecord2Value("");
				this.comparisonLog.setRecord2PreprocessedValue("");
			}
		}

		if (prepS1 != null && prepS2 != null) {

			// calculate similarity
			double similarity = sim.calculate(prepS1, prepS2);
			if (this.comparisonLog != null) {
				this.comparisonLog.setSimilarity(Double.toString(similarity));
			}
			
			//postprocessing
			if (this.isSquared())
				similarity *= similarity;

			
			
			if(similarity <= this.getThreshold()){
				similarity = 0;
			}
			
			if (this.comparisonLog != null) {
				this.comparisonLog.setPostprocessedSimilarity(Double.toString(similarity));
			}

			return similarity;
		}

		if (this.comparisonLog != null) {
			this.comparisonLog.setSimilarity(Double.toString(0.0));
		}
		
		return 0.0;
	}

	@Override
	public ComparatorLogger getComparisonLog() {
		return this.comparisonLog;
	}

	@Override
	public void setComparisonLog(ComparatorLogger comparatorLog) {
		this.comparisonLog = comparatorLog;
	}
	
	@Override
	public String getName(Correspondence<MatchableTableColumn, Matchable> schemaCorrespondence) 
	{	
		return String.format("%s~%s-T:%s/S:%s", 
				this.getClass().getSimpleName().replace("RowComparator", ""), this.getFirstSchemaElement(null).getHeader(),
				Double.toString(getThreshold()), Boolean.toString(isSquared()));
	}
}
