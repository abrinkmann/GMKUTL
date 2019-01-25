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

import java.util.HashSet;
import java.util.Set;

import com.wcohen.ss.api.Token;
import com.wcohen.ss.tokens.SimpleTokenizer;

import de.uni_mannheim.informatik.dws.GMKT.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.GMKT.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.matching.rules.Comparator;
import de.uni_mannheim.informatik.dws.winter.matching.rules.ComparatorLogger;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Record;
import de.uni_mannheim.informatik.dws.winter.similarity.list.MaximumOfContainment;
import edu.stanford.nlp.coref.data.WordLists;

/**
 * {@link Comparator} for {@link Record}s based on the {@link Attribute} values.
 * 
 * @author Alexander Brinkmann (albrinkm@mail.uni-mannheim.de)
 * 
 */
public class RowComparatorBagOfWordsContainment extends RowKBComparator {

	/**
	 * Create bag of words from either all or a single attribute.
	 * Before adding the tokens, stopwords are removed.
	 */
	private static final long serialVersionUID = 1L;

	private ComparatorLogger comparisonLog;
	private boolean all;
	private double threshold;
	private boolean lower;

	public RowComparatorBagOfWordsContainment(boolean all, double threshold, boolean lower) {
		super();
		this.all = all;
		this.threshold = threshold;
		this.lower = lower;
	}

	@Override
	public double compare(MatchableTableRow record1, MatchableTableRow record2,
			Correspondence<MatchableTableColumn, Matchable> schemaCorrespondence) {
		// tokenise strings
		SimpleTokenizer tok = new SimpleTokenizer(true, true);

		Set<String> s1 = new HashSet<>();
		Set<String> s2 = new HashSet<>();

		if (all) {
			for (MatchableTableColumn tc : record1.getSchema()) {
				if (record1.hasValue(tc)) {
					for (Token t : tok.tokenize(record1.getValue(tc).toString())) {
						if(this.lower){
							s1.add(t.getValue().toLowerCase());
						}
						else{
							s1.add(t.getValue());
						}
						
					}
				}
			}
		} else {
			if (record1.hasValue(schemaCorrespondence.getFirstRecord())) {
				MatchableTableColumn tc = schemaCorrespondence.getFirstRecord();
				for (Token t : tok.tokenize(record1.getValue(tc).toString())) {
					if(!WordLists.stopWordsEn.contains(t.getValue())){
						if(this.lower){
							s1.add(t.getValue().toLowerCase());
						}
						else{
							s1.add(t.getValue());
						}
					}
				}
			}

		}
		
		if (all) {
		for (MatchableTableColumn tc : record2.getSchema()) {
			if (record2.hasValue(tc)) {
				for (Token t : tok.tokenize(record2.getValue(tc).toString())) {
					if(!WordLists.stopWordsEn.contains(t.getValue())){
						if(this.lower){
							s2.add(t.getValue().toLowerCase());
						}
						else{
							s2.add(t.getValue());
						}
					}
				}
			}
		}
		} else {
			if (record2.hasValue(schemaCorrespondence.getSecondRecord())) {
				MatchableTableColumn tc = schemaCorrespondence.getSecondRecord();
				for (Token t : tok.tokenize(record2.getValue(tc).toString())) {
					if(!WordLists.stopWordsEn.contains(t.getValue())){
						if(this.lower){
							s2.add(t.getValue().toLowerCase());
						}
						else{
							s2.add(t.getValue());
						}
					}
				}
			}

		}
		

		if (this.comparisonLog != null) {
			this.comparisonLog.setComparatorName(getClass().getName());
			
			this.comparisonLog.setRecord1Value(s1.toString());
			this.comparisonLog.setRecord2Value(s2.toString());
		}

		// calculate score
		MaximumOfContainment<String> sim = new MaximumOfContainment<>();

		// calculate similarity
		double similarity = sim.calculate(s1, s2);

		if (this.comparisonLog != null) {
			this.comparisonLog.setSimilarity(Double.toString(similarity));
		}
		
		if(similarity <= this.threshold){
			similarity = 0;
		}
		
		if (this.comparisonLog != null) {
			this.comparisonLog.setPostprocessedSimilarity(Double.toString(similarity));
		}

		return similarity;
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
		if(this.all){
			return String.format("%s~all-T:%s/L:%s", 
					this.getClass().getSimpleName().replace("RowComparator", ""), Double.toString(threshold), Boolean.toString(this.lower));
		}
		return String.format("%s~%s-T:%s/L:%s", 
				this.getClass().getSimpleName().replace("RowComparator", ""), this.getFirstSchemaElement(null).getHeader(), Double.toString(threshold), Boolean.toString(this.lower));
		
	}

}
