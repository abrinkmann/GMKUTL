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

import com.wcohen.ss.api.Token;
import com.wcohen.ss.tokens.SimpleTokenizer;

import de.uni_mannheim.informatik.dws.GMKT.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.GMKT.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.generators.BlockingKeyGenerator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.MatchableValue;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;

/**
 * {@link BlockingKeyGenerator} for {@link Movie}s, which generates a blocking
 * key based on the title
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 * 
 */

public class BlockingKeyByNameGenerator
		extends BlockingKeyGenerator<MatchableTableRow, MatchableValue, MatchableTableRow> {

	private static final long serialVersionUID = 1L;
	private MatchableTableColumn matchableTableColumn;
	private SimpleTokenizer tok = new SimpleTokenizer(true, true);

	public BlockingKeyByNameGenerator(MatchableTableColumn matchableTableColumn) {
		this.matchableTableColumn = matchableTableColumn;
	}

	@Override
	public void generateBlockingKeys(MatchableTableRow record,
			Processable<Correspondence<MatchableValue, Matchable>> correspondences,
			DataIterator<Pair<String, MatchableTableRow>> resultCollector) {

		if (record.hasValue(this.matchableTableColumn)) {
			
			String s = (String) (record.getValue(this.matchableTableColumn));
			s = s.replaceAll("\\(.*\\)", "").toLowerCase();

			Token[] tokens = tok.tokenize(s);
			
			for (int i = 0; i <= 2 && i < tokens.length; i++) {
				resultCollector.next(new Pair<>(tokens[i].getValue(), record));
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.uni_mannheim.informatik.wdi.matching.blocking.generators.
	 * BlockingKeyGenerator#generateBlockingKeys(de.uni_mannheim.informatik.wdi.
	 * model.Matchable, de.uni_mannheim.informatik.wdi.model.Result,
	 * de.uni_mannheim.informatik.wdi.processing.DatasetIterator)
	 */

}
