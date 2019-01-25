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
package de.uni_mannheim.informatik.dws.GMKT.entitylinking;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import de.uni_mannheim.informatik.dws.GMKT.match.comparators.RowKBComparator;
import de.uni_mannheim.informatik.dws.GMKT.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.GMKT.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.GMKT.match.data.DataSets;
import de.uni_mannheim.informatik.dws.winter.matching.rules.LinearCombinationMatchingRule;
import de.uni_mannheim.informatik.dws.winter.matching.rules.WekaMatchingRule;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.MatchingGoldStandard;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.utils.WinterLogManager;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;

/**
 * @author Alexander Brinkmann (albrinkm@mail.uni-mannheim.de)
 *
 */
public class MatchingRuleGenerator {

	private static final Logger logger = WinterLogManager.getLogger();

	private double valueSimilarityThreshold;
	private DataSets kb;
	
	public MatchingRuleGenerator(DataSets kb, DataSets web, double valueSimilarityThreshold) {
		this.valueSimilarityThreshold = valueSimilarityThreshold;
		this.kb = kb;
	}

	public LinearCombinationMatchingRule<MatchableTableRow, MatchableTableColumn> createLinearCombinationMatchingRule(
			Processable<Correspondence<MatchableTableColumn, MatchableTableColumn>> correspondences, String logPath,
			String compPath, MatchingGoldStandard debugGoldstandard) {

		// create the matching rule
		LinearCombinationMatchingRule<MatchableTableRow, MatchableTableColumn> rule = new LinearCombinationMatchingRule<>(
				valueSimilarityThreshold);
		rule.activateDebugReport(logPath, 100, debugGoldstandard);

		HashMap<String, List<RowKBComparator>> compMap = null;
		try {
			compMap = loadComparatorsFromFile(compPath);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		for (Correspondence<MatchableTableColumn, MatchableTableColumn> cor : correspondences.get()) {
			MatchableTableColumn kbColumn = cor.getFirstRecord();

			List<RowKBComparator> comparators = compMap.get(kbColumn.getIdentifier());
			if (comparators != null) {

				try {
					for (RowKBComparator comp : comparators) {
						comp.setFirstSchemaElement(kbColumn);
						rule.addComparator(comp, cor.getSimilarityScore());
					}

				} catch (Exception e) {
					logger.error(e.getMessage());
					e.printStackTrace();
				}
			}

		}

		rule.normalizeWeights();

		logger.trace(String.format("%f <= %s", valueSimilarityThreshold, rule.toString()));

		return rule;
	}

	public WekaMatchingRule<MatchableTableRow, MatchableTableColumn> createLearnedMatchingRule(
			Processable<Correspondence<MatchableTableColumn, MatchableTableColumn>> correspondences, String debugPath,
			String classifierName, String parameters[], String compPath, MatchingGoldStandard debugGoldstandard, boolean featureSelection) {

		// create the matching rule
		WekaMatchingRule<MatchableTableRow, MatchableTableColumn> rule = new WekaMatchingRule<>(
				valueSimilarityThreshold, classifierName, parameters);
		//rule.activateDebugReport(debugPath, -1, debugGoldstandard);
		//rule.setForwardSelection(featureSelection);

		HashMap<String, List<RowKBComparator>> compMap = null;
		try {
			compMap = loadComparatorsFromFile(compPath);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		Set<MatchableTableColumn> setMatchableColums = new HashSet<MatchableTableColumn>();
		
		//Put correspondences into set, so that only one 
		for (Correspondence<MatchableTableColumn, MatchableTableColumn> cor : correspondences.get()) {
			setMatchableColums.add(cor.getFirstRecord());
			
		}
		
		Iterator<MatchableTableColumn> iterColumns = setMatchableColums.iterator();
		
		while(iterColumns.hasNext()){
			MatchableTableColumn kbColumn = iterColumns.next();
			List<RowKBComparator> comparators = compMap.get(kbColumn.getIdentifier());
			if (comparators != null) {
				try {
					for (RowKBComparator comp : comparators) {
						comp.setFirstSchemaElement(kbColumn);
						rule.addComparator(comp);
					}

				} catch (Exception e) {
					logger.error(e.getMessage());
					e.printStackTrace();
				}
			}

		}

		logger.trace(String.format("%f <= %s", valueSimilarityThreshold, rule.toString()));

		return rule;

	}

	public HashMap<String, List<RowKBComparator>> loadComparatorsFromFile(String filePath) throws IOException {
		HashMap<String, List<RowKBComparator>> comparators = new HashMap<String, List<RowKBComparator>>();
		List<RowKBComparator> compList = null;
		@SuppressWarnings("resource")
		BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), "UTF-8"));

		String line = in.readLine();

		while (line != null) {
			String[] values = line.split(",");

			if (values.length > 2) {
				String[] parameters = Arrays.copyOfRange(values, 2, values.length);
				@SuppressWarnings("rawtypes")
				Class[] cArg = new Class[(parameters.length / 2)];
				Object[] arguments = new Object[(parameters.length / 2)];

				int index = 0;
				for (int i = 0; i < parameters.length; i++) {
					if (parameters[i].equals("boolean")) {
						cArg[index] = boolean.class; // Second argument is of
														// *object* type String
						i++;
						arguments[index] = Boolean.parseBoolean(parameters[i]);
						index++;
					}
					if (parameters[i].equals("double")) {
						cArg[index] = double.class; // Second argument is of
													// *object* type String
						i++;
						arguments[index] = Double.parseDouble(parameters[i]);
						index++;
					}
					
					if (parameters[i].equals("int")) {
						cArg[index] = int.class; // Second argument is of
													// *object* type String
						i++;
						arguments[index] = Integer.parseInt(parameters[i]);
						index++;
					}
					
				}

				if (arguments.length == 1) {

					try {
						RowKBComparator comp = (RowKBComparator) Class.forName(values[1]).getConstructor(cArg)
								.newInstance(arguments[0]);
						
						String kbTableColumnIdentifier = "";
						String kbHeader = values[0].split("~")[1];
						String kbTableName = values[0].split("~")[0];
						HashMap<String, Integer> tableIndices = kb.getTableIndices();
						int kbTableId = tableIndices.get(kbTableName);
					
						for(TableColumn tc : kb.getTables().get(kbTableId).getSchema().getRecords()){
							if(tc.getHeader().equals(kbHeader)){
								kbTableColumnIdentifier = tc.getIdentifier();
								
								break;
							}
						}
						
						if (comparators.containsKey(kbTableColumnIdentifier)) {
							compList = comparators.get(kbTableColumnIdentifier);
						} else {
							compList = new ArrayList<RowKBComparator>();
						}
						compList.add(comp);
						comparators.put(kbTableColumnIdentifier, compList);

					} catch (Exception e) {
						logger.error("Class not found:" + values[1]);
						// e.printStackTrace();
					}
				}

				else if (arguments.length == 2) {
					try {
						RowKBComparator comp = (RowKBComparator) Class.forName(values[1]).getConstructor(cArg)
								.newInstance(arguments[0], arguments[1]);
						
						String kbTableColumnIdentifier = "";
						String kbHeader = values[0].split("~")[1];
						String kbTableName = values[0].split("~")[0];
						HashMap<String, Integer> tableIndices = kb.getTableIndices();
						int kbTableId = tableIndices.get(kbTableName);
					
						for(TableColumn tc : kb.getTables().get(kbTableId).getSchema().getRecords()){
							if(tc.getHeader().equals(kbHeader)){
								kbTableColumnIdentifier = tc.getIdentifier();
								
								break;
							}
						}
						
						if (comparators.containsKey(kbTableColumnIdentifier)) {
							compList = comparators.get(kbTableColumnIdentifier);
						} else {
							compList = new ArrayList<RowKBComparator>();
						}
						compList.add(comp);
						comparators.put(kbTableColumnIdentifier, compList);

					} catch (Exception e) {
						logger.error("Class not found:" + values[1]);
						// e.printStackTrace();
					}
				}
				else if (arguments.length == 3) {
					try {
						RowKBComparator comp = (RowKBComparator) Class.forName(values[1]).getConstructor(cArg)
								.newInstance(arguments[0], arguments[1], arguments[2]);
						
						String kbTableColumnIdentifier = "";
						String kbHeader = values[0].split("~")[1];
						String kbTableName = values[0].split("~")[0];
						HashMap<String, Integer> tableIndices = kb.getTableIndices();
						int kbTableId = tableIndices.get(kbTableName);
					
						for(TableColumn tc : kb.getTables().get(kbTableId).getSchema().getRecords()){
							if(tc.getHeader().equals(kbHeader)){
								kbTableColumnIdentifier = tc.getIdentifier();
								
								break;
							}
						}
						
						if (comparators.containsKey(kbTableColumnIdentifier)) {
							compList = comparators.get(kbTableColumnIdentifier);
						} else {
							compList = new ArrayList<RowKBComparator>();
						}
						compList.add(comp);
						comparators.put(kbTableColumnIdentifier, compList);

					} catch (Exception e) {
						logger.error("Class not found:" + values[1]);
						// e.printStackTrace();
					}
				}

				else if (arguments.length == 4) {
					try {
						RowKBComparator comp = (RowKBComparator) Class.forName(values[1]).getConstructor(cArg)
								.newInstance(arguments[0], arguments[1], arguments[2], arguments[3]);
						
						String kbTableColumnIdentifier = "";
						String kbHeader = values[0].split("~")[1];
						String kbTableName = values[0].split("~")[0];
						HashMap<String, Integer> tableIndices = kb.getTableIndices();
						int kbTableId = tableIndices.get(kbTableName);
					
						for(TableColumn tc : kb.getTables().get(kbTableId).getSchema().getRecords()){
							if(tc.getHeader().equals(kbHeader)){
								kbTableColumnIdentifier = tc.getIdentifier();
								
								break;
							}
						}
						
						if (comparators.containsKey(kbTableColumnIdentifier)) {
							compList = comparators.get(kbTableColumnIdentifier);
						} else {
							compList = new ArrayList<RowKBComparator>();
						}
						compList.add(comp);
						comparators.put(kbTableColumnIdentifier, compList);

					} catch (Exception e) {
						logger.error("Class not found:" + values[1]);
						// e.printStackTrace();
					}
				}

			} else {
				try {
					RowKBComparator comp = (RowKBComparator) Class.forName(values[1]).newInstance();
					
					String kbTableColumnIdentifier = "";
					String kbHeader = values[0].split("~")[1];
					String kbTableName = values[0].split("~")[0];
					HashMap<String, Integer> tableIndices = kb.getTableIndices();
					int kbTableId = tableIndices.get(kbTableName);
				
					for(TableColumn tc : kb.getTables().get(kbTableId).getSchema().getRecords()){
						if(tc.getHeader().equals(kbHeader)){
							kbTableColumnIdentifier = tc.getIdentifier();
							
							break;
						}
					}
					
					if (comparators.containsKey(kbTableColumnIdentifier)) {
						compList = comparators.get(kbTableColumnIdentifier);
					} else {
						compList = new ArrayList<RowKBComparator>();
					}
					compList.add(comp);
					comparators.put(kbTableColumnIdentifier, compList);

				} catch (Exception e) {
					logger.error("Class not found: " + values[1]);
					// e.printStackTrace();
				}
			}
			line = in.readLine();
		}

		return comparators;
	}
}
