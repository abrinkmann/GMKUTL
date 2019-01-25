package de.uni_mannheim.informatik.dws.GMKT.entitylinking;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.LinkedList;

import org.apache.logging.log4j.Logger;

import de.uni_mannheim.informatik.dws.GMKT.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.GMKT.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.GMKT.match.data.DataSetLoader;
import de.uni_mannheim.informatik.dws.GMKT.match.data.DataSets;
import de.uni_mannheim.informatik.dws.GMKT.reporting.ExperimentLogger;
import de.uni_mannheim.informatik.dws.GMKT.util.Constants;
import de.uni_mannheim.informatik.dws.GMKT.util.SchemaCorrespondenceSet;
import de.uni_mannheim.informatik.dws.winter.matching.MatchingEngine;
import de.uni_mannheim.informatik.dws.winter.matching.MatchingEvaluator;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.GoldStandardBlocker;
import de.uni_mannheim.informatik.dws.winter.matching.rules.LinearCombinationMatchingRule;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.MatchingGoldStandard;
import de.uni_mannheim.informatik.dws.winter.model.ParallelHashedDataSet;
import de.uni_mannheim.informatik.dws.winter.model.Performance;
import de.uni_mannheim.informatik.dws.winter.model.io.CSVCorrespondenceFormatter;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.utils.WinterLogManager;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;

public class BaselineGenerator {

	private static final Logger logger = WinterLogManager.getLogger();
	private DataSets kb;
	private DataSets web;
	private String knowledgeBaseClass;
	private String[] baselines = new String[] {"label", "bag_of_words", "linear_combination" };
	//private String[] baselines = new String[] { "containment" };

	private Collection<ExperimentLogger> experiments = new LinkedList<ExperimentLogger>();

	public BaselineGenerator(DataSets kb, DataSets web, String knowledgeBaseClass) {
		this.kb = kb;
		this.web = web;
		this.knowledgeBaseClass = knowledgeBaseClass;
	}

	public void run() {
		
		// Generate Data Sets
		DataSet<MatchableTableRow, MatchableTableColumn> kbRecords = new ParallelHashedDataSet<>(
				kb.getRecords().where((r) -> r.getTableId() == 0).get());

		for (Table dataSet : web.getTables().values()) {
			generateBaseline(dataSet.getPath(), kbRecords);
		}
		
		generateBaseline(Constants.all, kbRecords);
		
	}
	
	public void generateBaseline(String datasetPath, DataSet<MatchableTableRow, MatchableTableColumn> kbRecords){
		MatchingRuleGenerator ruleGenerator = new MatchingRuleGenerator(kb, web, 0.6);

		// Initialize Matching Engine
		MatchingEngine<MatchableTableRow, MatchableTableColumn> engine = new MatchingEngine<MatchableTableRow, MatchableTableColumn>();
		DataSetLoader loader = new DataSetLoader();
		
		DataSet<MatchableTableRow, MatchableTableColumn> ds = null;
		if(datasetPath.equals(Constants.all)){
			ds = web.getRecords();
		}
		else{
			int tableID = this.web.getTableIndices().get(datasetPath);
			Table dataSet = this.web.getTables().get(tableID);
			
			 ds = loader.loadDataSet(dataSet);
		}
		

		// Load gold standard
		MatchingGoldStandard gsTest = new MatchingGoldStandard();
		try {
			gsTest.loadFromCSVFile(new File(String.format("data/%s/goldstandard/silverstandard_%s_test_%s.csv",
					knowledgeBaseClass, knowledgeBaseClass, datasetPath.replace(".csv", ""))));

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		MatchingGoldStandard gsAll = new MatchingGoldStandard();
		try {
			gsAll.loadFromCSVFile(new File(String.format("data/%s/goldstandard/silverstandard_%s_all_%s.csv",
					knowledgeBaseClass, knowledgeBaseClass, datasetPath.replace(".csv", ""))));

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Load evaluator
		MatchingEvaluator<MatchableTableRow, MatchableTableColumn> evaluator = new MatchingEvaluator<MatchableTableRow, MatchableTableColumn>();

		for (String baselineName : baselines) {

			logger.info(String.format("Generate %s Baseline!", baselineName));
			LocalDateTime currentLabel = LocalDateTime.now();

			ExperimentLogger experiment1 = new ExperimentLogger(
					"baseline_" + baselineName + "_" + knowledgeBaseClass + "-" + datasetPath, currentLabel);

			experiment1.setValue(ExperimentLogger.KNOWLEDGEBASECLASS, knowledgeBaseClass);
			experiment1.setValue(ExperimentLogger.WEBTABLE, datasetPath);
			experiment1.setValue(ExperimentLogger.TRAINEDONWEBTABLE, datasetPath);

			// load correspondences
			SchemaCorrespondenceSet correspondenceSet = new SchemaCorrespondenceSet();
			try {
				if(datasetPath.equals(Constants.all)){
					correspondenceSet.loadSchemaCorrespondences(
							new File(String.format("data/%s/correspondences/baseline/schemaCorrespondences_%s.csv",
									knowledgeBaseClass, knowledgeBaseClass)),
							kb, web, null);
				} else {
					correspondenceSet.loadSchemaCorrespondences(
							new File(String.format("data/%s/correspondences/baseline/schemaCorrespondences_%s.csv",
									knowledgeBaseClass, knowledgeBaseClass)),
							kb, web, datasetPath);
				}
				
			} catch (IOException e) {
				e.printStackTrace();
			}

			Processable<Correspondence<MatchableTableColumn, MatchableTableColumn>> tableToKBCorrespondences = correspondenceSet
					.getSchemaCorrespondences();

			String debugPath = String.format("logs/%s/baseline/%s/matchingRule/%s_%s.csv", knowledgeBaseClass, datasetPath.replace(".csv", ""),
					baselineName, currentLabel.format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss-SSS")));
			String compPath = String.format("data/%s/comparators/baseline_%s_%s.csv", knowledgeBaseClass,
					baselineName, knowledgeBaseClass);

			LinearCombinationMatchingRule<MatchableTableRow, MatchableTableColumn> labelRule = ruleGenerator
					.createLinearCombinationMatchingRule(tableToKBCorrespondences, debugPath, compPath, gsAll);

			// create a blocker (blocking strategy)

			GoldStandardBlocker<MatchableTableRow, MatchableTableColumn, MatchableTableColumn> blocker = new GoldStandardBlocker<MatchableTableRow, MatchableTableColumn, MatchableTableColumn>(
					gsAll);

			// Execute the matching
			Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> correspondencesLabel = engine
					.runIdentityResolution(kbRecords, ds, tableToKBCorrespondences, labelRule, blocker);

			// write the correspondences to the output file
			try {
				new CSVCorrespondenceFormatter().writeCSV(
						new File(String.format(
								"data/%s/output/baseline/knowledgebase_%s_2_%s_correspondences_%s.csv",
								knowledgeBaseClass, knowledgeBaseClass, datasetPath, baselineName)),
						correspondencesLabel);
			} catch (IOException e) {
				e.printStackTrace();
			}

			// evaluate result
			Performance perfTestLabel = evaluator.evaluateMatching(correspondencesLabel.get(), gsTest);

			// print the evaluation result
			logger.info(String.format("Knowledge Base %s <-> %s", knowledgeBaseClass, datasetPath));
			logger.info(String.format("Precision: %.4f", perfTestLabel.getPrecision()));
			logger.info(String.format("Recall: %.4f", perfTestLabel.getRecall()));
			logger.info(String.format("F1: %.4f", perfTestLabel.getF1()));

			// log results
			experiment1.setValue(ExperimentLogger.PRECISION, Double.toString(perfTestLabel.getPrecision()));
			experiment1.setValue(ExperimentLogger.RECALL, Double.toString(perfTestLabel.getRecall()));
			experiment1.setValue(ExperimentLogger.F1, Double.toString(perfTestLabel.getF1()));

			this.experiments.add(experiment1);
		}
	}

	public Collection<ExperimentLogger> getResult() {
		return this.experiments;
	}
}
