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
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;

/**
 * @author Alexander Brinkmann (albrinkm@mail.uni-mannheim.de)
 *
 */
public abstract class StringComparator extends RowKBComparator {


	private static final long serialVersionUID = 1L;
	private boolean removeBrackets = false;
	private boolean lowerCase = false;
	
	private double threshold;
	private boolean squared;
	
	public StringComparator() {
		
	}
	
	public StringComparator(double threshold, boolean squared, boolean removeBrackets, boolean lowerCase) {
		this.threshold = threshold;
		this.removeBrackets = removeBrackets;
		this.lowerCase = lowerCase;
		this.squared = squared;
	}
	
	public boolean isRemoveBrackets() {
		return removeBrackets;
	}

	public boolean isLowerCase() {
		return lowerCase;
	}

	/**
	 * @param removeBrackets the removeBrackets to set
	 */
	public void setRemoveBrackets(boolean removeBrackets) {
		this.removeBrackets = removeBrackets;
	}
	
	/**
	 * @param lowerCase the lowerCase to set
	 */
	public void setLowerCase(boolean lowerCase) {
		this.lowerCase = lowerCase;
	}

	protected String preprocess(String s) {
		
		if(s==null) {
			return null;
		} else {
			
			if(removeBrackets) {
				s = s.replaceAll("\\(.*\\)", "");
			}
			
			if(lowerCase) {
				s = s.toLowerCase();
			}
			
			return s;
		}
	}

	public boolean isSquared() {
		return squared;
	}

	public void setSquared(boolean squared) {
		this.squared = squared;
	}
	
	public double getThreshold() {
		return threshold;
	}

	public void setThreshold(double threshold) {
		this.threshold = threshold;
	}
	
	@Override
	public String getName(Correspondence<MatchableTableColumn, Matchable> schemaCorrespondence) 
	{	
		return String.format("%s~%s-T:%s/S:%s/B:%s/L:%s", 
				this.getClass().getSimpleName().replace("RowComparator", ""), this.getFirstSchemaElement(null).getHeader(),
				Double.toString(getThreshold()), Boolean.toString(isSquared()), Boolean.toString(isRemoveBrackets()), Boolean.toString(isLowerCase()));
	}

}
