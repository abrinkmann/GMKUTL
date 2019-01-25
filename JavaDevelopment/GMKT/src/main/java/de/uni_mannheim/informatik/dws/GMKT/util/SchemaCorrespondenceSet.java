package de.uni_mannheim.informatik.dws.GMKT.util;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.logging.log4j.Logger;

import au.com.bytecode.opencsv.CSVReader;
import de.uni_mannheim.informatik.dws.GMKT.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.GMKT.match.data.DataSets;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.parallel.ParallelProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.utils.WinterLogManager;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;

/**
 * Represents a set of correspondences (from the schema Match)
 * 
 * @author Alexander Brinkmann (albrinkm@mail.uni-mannheim.de)
 * 
 * @param <RecordType>	the type that represents a record
 */

public class SchemaCorrespondenceSet {
	
	private static final Logger logger = WinterLogManager.getLogger();
	
	private Processable<Correspondence<MatchableTableColumn,MatchableTableColumn>> cors = null;
	
	/**
	 * Loads correspondences from a file.
	 * 
	 * @param correspondenceFile	the to load from
	 * @param kb					the knowledge base that contains the column on the left-hand side of the correspondences
	 * @param web					the web table that contains the column on the right-hand side of the correspondences
	 * @throws IOException			thrown if there is a problem loading the file
	 */
	public void loadSchemaCorrespondences(File correspondenceFile,
			DataSets kb, DataSets web, String inputTable)
			throws IOException {
		
		CSVReader reader = new CSVReader(new FileReader(correspondenceFile));
		
		cors = new ParallelProcessableCollection<Correspondence<MatchableTableColumn,MatchableTableColumn>>();
		String[] values = null;
		
		while ((values = reader.readNext()) != null) {
			// check if the ids exist in the provided datasets
			
			String tableName = inputTable;
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
			
			if (kb.getSchema().getRecord(kbTableColumnIdentifier) == null) {
				logger.error(String.format(
						"Column %s not found in knowledge base", kbTableColumnIdentifier));
				continue;
			}
			
			if(inputTable == null || values[1].contains(inputTable)){
				// Match ALL columns
				if(values[1].contains("~ALL")){
					for(MatchableTableColumn tc : web.getSchema().get()){
						// load correspondences
						double similarity = 1.0;
						if(values.length > 2){
							similarity = Double.parseDouble(values[2]);
						}
						
						Correspondence<MatchableTableColumn, MatchableTableColumn> cor = new Correspondence<>(kb.getSchema().getRecord(kbTableColumnIdentifier), tc, similarity);
						if(cor.getFirstRecord() == null || cor.getSecondRecord() == null){
							logger.error(String.format("Issue with schema correspondence line: %s", values.toString()));
						}
						cors.add(cor);
					}
				}
				else if(values[1].contains("~DUMMY")){
					double similarity = 1.0;
					if(values.length > 2){
						similarity = Double.parseDouble(values[2]);
					}
				
					// load correspondences +  Add random record
					if(inputTable == null){
						tableName = values[1].replace("~DUMMY", "");
					}
					
					int tableId = web.getTableIndices().get(tableName);
					TableColumn randomTC = web.getTables().get(tableId).getSchema().getRecords().iterator().next();
					String randomTableColumnIdentifier = randomTC.getIdentifier();
					
					if (web.getSchema().getRecord(randomTableColumnIdentifier) == null) {
						logger.error(String.format(
								"Column %s not found in provided web tables", values[1]));
						continue;
					}
					
					Correspondence<MatchableTableColumn, MatchableTableColumn> cor = new Correspondence<>(kb.getSchema().getRecord(kbTableColumnIdentifier),web.getSchema().getRecord(randomTableColumnIdentifier) , similarity);
					if(cor.getFirstRecord() == null || cor.getSecondRecord() == null){
						logger.error(String.format("Issue with schema correspondence line: %s", values.toString()));
					}
					cors.add(cor);
				}
				else{
					String header = values[1].split("~")[1];
					if(inputTable == null){
						tableName = values[1].split("~")[0];
					}
					
					int tableId = web.getTableIndices().get(tableName);
					String tableColumnIdentifier = "";
					for(TableColumn tc : web.getTables().get(tableId).getSchema().getRecords()){
						if(tc.getHeader().equals(header)){
							tableColumnIdentifier = tc.getIdentifier();
							break;
						}
					}
					
					if (web.getSchema().getRecord(tableColumnIdentifier) == null) {
						logger.error(String.format(
								"Column %s not found in provided web tables", values[1]));
						continue;
					}
					double similarity = 1.0;
					if(values.length > 2){
						similarity = Double.parseDouble(values[2]);
					}
				
					// load correspondences
					Correspondence<MatchableTableColumn, MatchableTableColumn> cor = new Correspondence<>(kb.getSchema().getRecord(kbTableColumnIdentifier), web.getSchema().getRecord(tableColumnIdentifier), similarity);
					if(cor.getFirstRecord() == null || cor.getSecondRecord() == null){
						logger.error(String.format("Issue with schema correspondence line: %s", values.toString()));
					}
					cors.add(cor);
				}
			}
		}

		reader.close();
	}
	
	public Processable<Correspondence<MatchableTableColumn,MatchableTableColumn>> getSchemaCorrespondences(){
		return this.cors;
	}
	

}
