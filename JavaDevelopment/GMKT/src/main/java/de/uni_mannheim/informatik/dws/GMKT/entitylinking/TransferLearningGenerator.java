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
import de.uni_mannheim.informatik.dws.GMKT.util.DecisionTreeInspector;
import de.uni_mannheim.informatik.dws.GMKT.util.SchemaCorrespondenceSet;
import de.uni_mannheim.informatik.dws.GMKT.util.SimpleLogisticInspector;
import de.uni_mannheim.informatik.dws.winter.matching.MatchingEngine;
import de.uni_mannheim.informatik.dws.winter.matching.MatchingEvaluator;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.RuleLearner;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.GoldStandardBlocker;
import de.uni_mannheim.informatik.dws.winter.matching.rules.WekaMatchingRule;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.MatchingGoldStandard;
import de.uni_mannheim.informatik.dws.winter.model.ParallelHashedDataSet;
import de.uni_mannheim.informatik.dws.winter.model.Performance;
import de.uni_mannheim.informatik.dws.winter.model.io.CSVCorrespondenceFormatter;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.utils.WinterLogManager;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import weka.classifiers.functions.SimpleLogistic;
import weka.classifiers.trees.J48;

public class TransferLearningGenerator {

	private static final Logger logger = WinterLogManager.getLogger();
	private DataSets kb;
	private DataSets datasets;
	private String knowledgeBaseClass;
	private String classifierName;
	private String[] parameters;
	private boolean featureSelection;

	private Collection<ExperimentLogger> experiments = new LinkedList<ExperimentLogger>();

	public TransferLearningGenerator(DataSets kb, DataSets datasets, String knowledgeBaseClass,
			String classifierName, String parameters[], boolean featureSelection) {
		this.kb = kb;
		this.datasets = datasets;
		this.knowledgeBaseClass = knowledgeBaseClass;
		this.classifierName = classifierName;
		this.parameters = parameters;
		this.featureSelection = featureSelection;
	}

	public void run() {

		// Generate Data Sets
		DataSet<MatchableTableRow, MatchableTableColumn> kbRecords = new ParallelHashedDataSet<>(
				kb.getRecords().where((r) -> r.getTableId() == 0).get());
		
		
		// Retrieve table
		for (Table sourceWebTable : datasets.getTables().values()) {

			// ******************************************
			// Learn matching Rule
			// ******************************************
			
			 WekaMatchingRule<MatchableTableRow, MatchableTableColumn> rule = executeLearningMatchingRuleExperiment(sourceWebTable.getPath(), kbRecords);
			 
			 for (Table transferWebTable : datasets.getTables().values()) { 
				 if(!transferWebTable.getPath().equals(sourceWebTable.getPath())) {
			 // ****************************************** 
			// transfer to a single other web table 
			 // ******************************************
					 transferMatchingRuleExperiment(sourceWebTable.
			  getPath(), transferWebTable.getPath(), kbRecords, rule); 
			  } 
			 }
			 
			 transferMatchingRuleExperiment(sourceWebTable.
					  getPath(), Constants.all, kbRecords, rule);
		}
		
		// ******************************************
		// Apply rule to all a shared gs
		// ******************************************
		WekaMatchingRule<MatchableTableRow, MatchableTableColumn> ruleAll = executeLearningMatchingRuleExperiment(Constants.all,kbRecords);

		for (Table transferWebTable : datasets.getTables().values()) {
			// ******************************************
			// transfer to a single other web table
			// ******************************************
			transferMatchingRuleExperiment(Constants.all, transferWebTable.getPath(), kbRecords, ruleAll);
		}
	}

	public Collection<ExperimentLogger> getResult() {
		return this.experiments;
	}

	public WekaMatchingRule<MatchableTableRow, MatchableTableColumn> executeLearningMatchingRuleExperiment(
			String sourceWebPath, DataSet<MatchableTableRow, MatchableTableColumn> kbRecords) {
		// ******************************************
		// generate learned matching rule
		// ******************************************

		logger.info("Generate learned matching rule!");
		LocalDateTime currentLabel = LocalDateTime.now();

		ExperimentLogger experiment = new ExperimentLogger(
				"learned_matchingRule_" + knowledgeBaseClass + "-" + sourceWebPath, currentLabel);

		experiment.setValue(ExperimentLogger.KNOWLEDGEBASECLASS, knowledgeBaseClass);
		experiment.setValue(ExperimentLogger.WEBTABLE, sourceWebPath);
		experiment.setValue(ExperimentLogger.TRAINEDONWEBTABLE, sourceWebPath);

		// Load Records of webtable
		DataSetLoader loader = new DataSetLoader();
		DataSet<MatchableTableRow, MatchableTableColumn> ds = null;
		if(sourceWebPath.equals(Constants.all)){
			ds = this.datasets.getRecords();
		} else {
			int tableId = this.datasets.getTableIndices().get(sourceWebPath);
			Table dataSet = this.datasets.getTables().get(tableId);
			
			ds = loader.loadDataSet(dataSet);
		}

				
		// Load gold standards
		MatchingGoldStandard gsTrain = new MatchingGoldStandard();
		try {
			gsTrain.loadFromCSVFile(new File(String.format("data/%s/goldstandard/silverstandard_%s_train_%s.csv",
					knowledgeBaseClass, knowledgeBaseClass, sourceWebPath.replace(".csv", ""))));

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		MatchingGoldStandard gsTest = new MatchingGoldStandard();
		try {
			gsTest.loadFromCSVFile(new File(String.format("data/%s/goldstandard/silverstandard_%s_test_%s.csv",
					knowledgeBaseClass, knowledgeBaseClass, sourceWebPath.replace(".csv", ""))));

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		MatchingGoldStandard gsAll = new MatchingGoldStandard();
		try {
			gsAll.loadFromCSVFile(new File(String.format("data/%s/goldstandard/silverstandard_%s_all_%s.csv",
					knowledgeBaseClass, knowledgeBaseClass, sourceWebPath.replace(".csv", ""))));

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

		// load correspondences
		SchemaCorrespondenceSet correspondenceSet = new SchemaCorrespondenceSet();
		try {
			if(sourceWebPath.equals(Constants.all)){
				correspondenceSet.loadSchemaCorrespondences(
						new File(String.format("data/%s/correspondences/transferLearning/schemaCorrespondences_%s.csv",
								knowledgeBaseClass, knowledgeBaseClass)),
						kb, datasets, null);
			} else {
				correspondenceSet.loadSchemaCorrespondences(
						new File(String.format("data/%s/correspondences/transferLearning/schemaCorrespondences_%s.csv",
								knowledgeBaseClass, knowledgeBaseClass)),
						kb, datasets, sourceWebPath);
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}

		Processable<Correspondence<MatchableTableColumn, MatchableTableColumn>> sourcetableToKBCorrespondences = correspondenceSet
				.getSchemaCorrespondences();

		String debugPath = String.format("logs/%s/learning/%s/matchingRule/%s_%s.csv", knowledgeBaseClass,
				sourceWebPath.replace(".csv", ""),
				currentLabel.format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss-SSS")),
				sourceWebPath.replace(".csv", ""));
		String compPath = String.format("data/%s/comparators/transferLearning_%s.csv", knowledgeBaseClass,
				knowledgeBaseClass);
		
		MatchingRuleGenerator ruleGenerator = new MatchingRuleGenerator(kb, datasets, 0.25);
		WekaMatchingRule<MatchableTableRow, MatchableTableColumn> rule = ruleGenerator.createLearnedMatchingRule(
				sourcetableToKBCorrespondences, debugPath, this.classifierName, this.parameters, compPath, gsAll, this.featureSelection);

		// create a blocker (blocking strategy) --> Make sure that this
		// strategy is consistent with all other experiments
		GoldStandardBlocker<MatchableTableRow, MatchableTableColumn, MatchableTableColumn> blocker = new GoldStandardBlocker<MatchableTableRow, MatchableTableColumn, MatchableTableColumn>(
				gsTest);

		// learning Matching rule
		RuleLearner<MatchableTableRow, MatchableTableColumn> learner = new RuleLearner<>();
		learner.learnMatchingRule(kbRecords, ds, sourcetableToKBCorrespondences, rule, gsTrain);

		// Execute the matching
		MatchingEngine<MatchableTableRow, MatchableTableColumn> engine = new MatchingEngine<MatchableTableRow, MatchableTableColumn>();
		Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> correspondencesLabel = engine
				.runIdentityResolution(kbRecords, ds, sourcetableToKBCorrespondences, rule, blocker);

		// write the correspondences to the output file
		try {
			new CSVCorrespondenceFormatter().writeCSV(
					new File(String.format("data/%s/output/learning/knowledgebase_%s_2_%s_correspondences.csv",
							knowledgeBaseClass, knowledgeBaseClass, sourceWebPath.replace(".csv", ""))),
					correspondencesLabel);
		} catch (IOException e) {
			e.printStackTrace();
		}

		// evaluate result
		MatchingEvaluator<MatchableTableRow, MatchableTableColumn> evaluator = new MatchingEvaluator<MatchableTableRow, MatchableTableColumn>();
		Performance perfTestLabel = evaluator.evaluateMatching(correspondencesLabel.get(), gsTest);

		// print the evaluation result
		logger.info(String.format("Knowledge Base %s <-> %s", knowledgeBaseClass, sourceWebPath));
		logger.info(String.format("Precision: %.4f", perfTestLabel.getPrecision()));
		logger.info(String.format("Recall: %.4f", perfTestLabel.getRecall()));
		logger.info(String.format("F1: %.4f", perfTestLabel.getF1()));

		// log results
		experiment.setValue(ExperimentLogger.PRECISION, Double.toString(perfTestLabel.getPrecision()));
		experiment.setValue(ExperimentLogger.RECALL, Double.toString(perfTestLabel.getRecall()));
		experiment.setValue(ExperimentLogger.F1, Double.toString(perfTestLabel.getF1()));
		
		// Add rule information
		experiment.setValue(ExperimentLogger.TRAINEDMODEL, rule.getClassifier().getClass().getSimpleName());
		
		this.experiments.add(experiment);
		
		if(rule.getClassifier() instanceof J48){
		
			String importancePath = String.format("logs/%s/learning/%s/matchingRule/PLACEHOLDER_importance_J48_%s_%s.csv", knowledgeBaseClass,
				sourceWebPath.replace(".csv", ""),
				currentLabel.format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss-SSS")),
				sourceWebPath.replace(".csv", ""));
			DecisionTreeInspector.inspectDecisionTree((J48) rule.getClassifier(), sourceWebPath, importancePath);
		}
		
		if(rule.getClassifier() instanceof SimpleLogistic){
			
			String importancePath = String.format("logs/%s/learning/%s/matchingRule/PLACEHOLDER_importance_SimpleLogistic_%s_%s.csv", knowledgeBaseClass,
					sourceWebPath.replace(".csv", ""),
					currentLabel.format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss-SSS")),
					sourceWebPath.replace(".csv", ""));
				SimpleLogisticInspector.inspectDecisionTree((SimpleLogistic) rule.getClassifier(), sourceWebPath, importancePath);
		
		}
		return rule;
	}

	public void transferMatchingRuleExperiment(String sourceWebTablePath, String transferWebTablePath,
			DataSet<MatchableTableRow, MatchableTableColumn> kbRecords,
			WekaMatchingRule<MatchableTableRow, MatchableTableColumn> rule) {

		logger.info(String.format("Transfer learned matching rule from %s to %s!", sourceWebTablePath,
				transferWebTablePath));
		LocalDateTime currentTransfer = LocalDateTime.now();

		ExperimentLogger experimentTransfer = new ExperimentLogger(
				"transfer_matchingRule_" + knowledgeBaseClass + "-" + sourceWebTablePath, currentTransfer);

		experimentTransfer.setValue(ExperimentLogger.KNOWLEDGEBASECLASS, knowledgeBaseClass);
		experimentTransfer.setValue(ExperimentLogger.WEBTABLE, transferWebTablePath);
		experimentTransfer.setValue(ExperimentLogger.TRAINEDONWEBTABLE, sourceWebTablePath);
		experimentTransfer.setValue(ExperimentLogger.TRAINEDMODEL, rule.getClassifier().getClass().getSimpleName());

		// Rename columns if possible! --> Use CorrespondenceSet to
		// retrieve renamings +
		SchemaCorrespondenceSet correspondenceSet = new SchemaCorrespondenceSet();

		try {
			if(transferWebTablePath.equals(Constants.all)){
				correspondenceSet.loadSchemaCorrespondences(
						new File(String.format("data/%s/correspondences/transferLearning/schemaCorrespondences_%s.csv",
								knowledgeBaseClass, knowledgeBaseClass)),
						kb, datasets, null);
			} else {
			correspondenceSet.loadSchemaCorrespondences(
					new File(String.format("data/%s/correspondences/transferLearning/schemaCorrespondences_%s.csv",
							knowledgeBaseClass, knowledgeBaseClass)),
					kb, datasets, transferWebTablePath);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		Processable<Correspondence<MatchableTableColumn, MatchableTableColumn>> transfertableToKBCorrespondences = correspondenceSet
				.getSchemaCorrespondences();

		//load records from data sets
		DataSetLoader loader = new DataSetLoader();
		DataSet<MatchableTableRow, MatchableTableColumn> dsTransfer = null;
		if(transferWebTablePath.equals(Constants.all)){
			dsTransfer = this.datasets.getRecords();
		} else {
			int tableId = this.datasets.getTableIndices().get(transferWebTablePath);
			Table dataSet = this.datasets.getTables().get(tableId);
			dsTransfer = loader.loadDataSet(dataSet);
		}
		
		MatchingGoldStandard gsTestTransfer = new MatchingGoldStandard();
		try {
			gsTestTransfer.loadFromCSVFile(new File(String.format("data/%s/goldstandard/silverstandard_%s_test_%s.csv",
					knowledgeBaseClass, knowledgeBaseClass, transferWebTablePath.replace(".csv", ""))));

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Define blocker
		GoldStandardBlocker<MatchableTableRow, MatchableTableColumn, MatchableTableColumn> blockerTransfer = new GoldStandardBlocker<MatchableTableRow, MatchableTableColumn, MatchableTableColumn>(
				gsTestTransfer);

		// Execute the matching
		// Initialize Matching Engine
		MatchingEngine<MatchableTableRow, MatchableTableColumn> engine = new MatchingEngine<MatchableTableRow, MatchableTableColumn>();
		Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> correspondencesTransfer = engine
				.runIdentityResolution(kbRecords, dsTransfer, transfertableToKBCorrespondences, rule, blockerTransfer);

		// write the correspondences to the output file
		try {
			new CSVCorrespondenceFormatter().writeCSV(
					new File(String.format("data/%s/output/transfer/knowledgebase_%s_2_%s_correspondences.csv",
							knowledgeBaseClass, knowledgeBaseClass, transferWebTablePath.replace(".csv", ""))),
					correspondencesTransfer);
		} catch (IOException e) {
			e.printStackTrace();
		}

		// evaluate result
		// Load evaluator
		MatchingEvaluator<MatchableTableRow, MatchableTableColumn> evaluator = new MatchingEvaluator<MatchableTableRow, MatchableTableColumn>();
		Performance perfTestTransfer = evaluator.evaluateMatching(correspondencesTransfer.get(), gsTestTransfer);

		// print the evaluation result
		logger.info(String.format("Knowledge Base %s <-> %s", knowledgeBaseClass, transferWebTablePath));
		logger.info(String.format("Precision: %.4f", perfTestTransfer.getPrecision()));
		logger.info(String.format("Recall: %.4f", perfTestTransfer.getRecall()));
		logger.info(String.format("F1: %.4f", perfTestTransfer.getF1()));

		// log results
		experimentTransfer.setValue(ExperimentLogger.PRECISION, Double.toString(perfTestTransfer.getPrecision()));
		experimentTransfer.setValue(ExperimentLogger.RECALL, Double.toString(perfTestTransfer.getRecall()));
		experimentTransfer.setValue(ExperimentLogger.F1, Double.toString(perfTestTransfer.getF1()));

		this.experiments.add(experimentTransfer);
	}

}
