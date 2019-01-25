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

import de.uni_mannheim.informatik.dws.GMKT.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.GMKT.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.matching.rules.Comparator;
import de.uni_mannheim.informatik.dws.winter.matching.rules.ComparatorLogger;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Record;
import de.uni_mannheim.informatik.dws.winter.similarity.numeric.PercentageSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.string.TokenizingJaccardSimilarity;

/**
 * {@link Comparator} for {@link Record}s based on the {@link Attribute} values,
 * and their {@link TokenizingJaccardSimilarity} similarity.
 * 
 * @author Alexander Brinkmann (albrinkm@mail.uni-mannheim.de)
 * 
 */
public class RowComparatorPercentageSimilarity extends RowKBComparator{

	private static final long serialVersionUID = 1L;
	PercentageSimilarity sim = null;

	private ComparatorLogger comparisonLog;
	private boolean squared;
	private double threshold;

	public RowComparatorPercentageSimilarity(double threshold, boolean squared) {
		this.sim = new PercentageSimilarity(1);
		this.squared = squared;
		this.threshold = threshold;
	}

	@Override
	public double compare(MatchableTableRow record1, MatchableTableRow record2,
			Correspondence<MatchableTableColumn, Matchable> schemaCorrespondence) {

		if (this.comparisonLog != null) {
			this.comparisonLog.setComparatorName(getClass().getName());
		}

		Double d1 = null;
		Double d2 = null;

		if (record1.hasValue(schemaCorrespondence.getFirstRecord())) {
			d1 = Double.valueOf(record1.getValue(schemaCorrespondence.getFirstRecord()).toString());

			if (this.comparisonLog != null) {
				this.comparisonLog.setRecord1Value(Double.toString(d1));
				this.comparisonLog.setRecord1PreprocessedValue(Double.toString(d1));
			}
		}
		else{
			if (this.comparisonLog != null) {
				this.comparisonLog.setRecord1Value("");
				this.comparisonLog.setRecord1PreprocessedValue("");
			}
		}

		if (record2.hasValue(schemaCorrespondence.getSecondRecord())) {
			d2 = Double.valueOf(record2.getValue(schemaCorrespondence.getSecondRecord()).toString());


			if (this.comparisonLog != null) {
				this.comparisonLog.setRecord2Value(Double.toString(d2));
				this.comparisonLog.setRecord2PreprocessedValue(Double.toString(d2));
			}
		}
		else{
			if (this.comparisonLog != null) {
				this.comparisonLog.setRecord2Value("");
				this.comparisonLog.setRecord2PreprocessedValue("");
			}
		}

		if (d1 != null && d2 != null) {
			// calculate similarity
			double similarity = sim.calculate(d1, d2);

			if (this.comparisonLog != null) {
				this.comparisonLog.setSimilarity(Double.toString(similarity));
			}

			// postprocessing
			if (this.squared)
				similarity *= similarity;
			
			if(similarity <= this.threshold){
				similarity = 0;
			}
			
			if (this.comparisonLog != null) {
				this.comparisonLog.setPostprocessedSimilarity(Double.toString(similarity));
			}
			

			return similarity;
		}
		if (this.comparisonLog != null) {
			this.comparisonLog.setSimilarity("0.0");

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
				Double.toString(this.threshold), Boolean.toString(this.squared));
	}

}
