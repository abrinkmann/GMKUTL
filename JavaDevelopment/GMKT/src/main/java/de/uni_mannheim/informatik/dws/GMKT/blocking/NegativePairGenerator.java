package de.uni_mannheim.informatik.dws.GMKT.blocking;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

import org.apache.logging.log4j.Logger;

import de.uni_mannheim.informatik.dws.GMKT.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.GMKT.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.GMKT.match.data.DataSetLoader;
import de.uni_mannheim.informatik.dws.GMKT.match.data.DataSets;
import de.uni_mannheim.informatik.dws.GMKT.reporting.ExperimentLogger;
import de.uni_mannheim.informatik.dws.GMKT.util.SchemaCorrespondenceSet;
import de.uni_mannheim.informatik.dws.winter.matching.MatchingEngine;
import de.uni_mannheim.informatik.dws.winter.matching.MatchingEvaluator;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.BlockingKeyIndexer.VectorCreationMethod;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.MatchableValue;
import de.uni_mannheim.informatik.dws.winter.model.MatchingGoldStandard;
import de.uni_mannheim.informatik.dws.winter.model.ParallelHashedDataSet;
import de.uni_mannheim.informatik.dws.winter.model.Performance;
import de.uni_mannheim.informatik.dws.winter.model.io.CSVCorrespondenceFormatter;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.parallel.ParallelProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.similarity.vectorspace.VectorSpaceMaximumOfContainmentSimilarity;
import de.uni_mannheim.informatik.dws.winter.utils.WinterLogManager;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;

public class NegativePairGenerator {

	private static final Logger logger = WinterLogManager.getLogger();
	private DataSets kb;
	private DataSets web;
	private String knowledgeBaseClass;
	private double similarityThreshold;

	private Collection<ExperimentLogger> experiments = new LinkedList<ExperimentLogger>();

	public NegativePairGenerator(DataSets kb, DataSets web, String knowledgeBaseClass, double similarityThreshold) {
		this.kb = kb;
		this.web = web;
		this.knowledgeBaseClass = knowledgeBaseClass;
		this.similarityThreshold = similarityThreshold;
	}

	public void run() {

		negativePairSelection();

	}

	private void negativePairSelection() {

		for (Table webTable : web.getTables().values()) {

			// Initialize Matching Engine
			MatchingEngine<MatchableTableRow, MatchableTableColumn> engine = new MatchingEngine<MatchableTableRow, MatchableTableColumn>();
			DataSetLoader loader = new DataSetLoader();

			// Generate Data Sets
			DataSet<MatchableTableRow, MatchableTableColumn> kbRecords = new ParallelHashedDataSet<>(
					kb.getRecords().where((r) -> r.getTableId() == 0).get());
			DataSet<MatchableTableRow, MatchableTableColumn> ds = loader.loadDataSet(webTable);
			
			
			// Load gold standard
			MatchingGoldStandard gsTrue = new MatchingGoldStandard();
			try {
				gsTrue.loadFromCSVFile(new File(String.format("data/%s/goldstandard/goldstandard_%s_true_%s",
						knowledgeBaseClass, knowledgeBaseClass, webTable.getPath())));

			} catch (IOException e) {
				e.printStackTrace();
			}
			

			// ******************************************
			// generate indexing strategy
			// ******************************************

			logger.info("Generate Negative Pairs!");

			// load correspondences
			SchemaCorrespondenceSet correspondenceSet = new SchemaCorrespondenceSet();
			try {
				correspondenceSet.loadSchemaCorrespondences(new File(
						String.format("data/%s/correspondences/indexingStrategy/labelSchemaCorrespondences_%s.csv",
								knowledgeBaseClass, knowledgeBaseClass)),
						kb, web, webTable.getPath());
			} catch (IOException e) {
				e.printStackTrace();
			}

			Processable<Correspondence<MatchableTableColumn, MatchableTableColumn>> tableToKBCorrespondences = correspondenceSet
					.getSchemaCorrespondences();

			// ******************************************
			// generate indexing strategy
			// ******************************************
			if(tableToKBCorrespondences.get().isEmpty()){
				logger.error(String.format("No correspondences found for %s!" , webTable.getPath()));
			}
			else{
			Processable<Correspondence<MatchableTableRow, MatchableValue>> correspondencesTermFrequencies = engine
					.runVectorBasedIdentityResolution(kbRecords, ds,
							new BlockingKeyByNameGenerator(
									tableToKBCorrespondences.get().iterator().next().getFirstRecord()),
							new BlockingKeyByNameGenerator(
									tableToKBCorrespondences.get().iterator().next().getSecondRecord()),
							VectorCreationMethod.BinaryTermOccurrences, new VectorSpaceMaximumOfContainmentSimilarity(),
							similarityThreshold);
			
			//Filter correspondences for knowledge base class citation - HACK for simplicity purposes
			if(this.knowledgeBaseClass.equals("citation")){
				Processable<Correspondence<MatchableTableRow, MatchableValue>> correspondencesTermFrequenciesTmp = correspondencesTermFrequencies;
				correspondencesTermFrequencies = new ParallelProcessableCollection<Correspondence<MatchableTableRow, MatchableValue>>();
				
				// Load gold standard
				MatchingGoldStandard gsComplete = new MatchingGoldStandard();
				try {
					gsComplete.loadFromCSVFile(new File(String.format("data/%s/goldstandard/goldstandard_%s_complete.csv",
							knowledgeBaseClass, knowledgeBaseClass)));

				} catch (IOException e) {
					e.printStackTrace();
				}
				
				
				for(Correspondence<MatchableTableRow, MatchableValue> cor : correspondencesTermFrequenciesTmp.get()){
					
					if(gsComplete.containsNegative(cor.getFirstRecord().getIdentifier(), cor.getSecondRecord().getIdentifier()) ||
							gsComplete.containsPositive(cor.getFirstRecord().getIdentifier(), cor.getSecondRecord().getIdentifier())){
					
					if(cor.getFirstRecord().getIdentifier().contains("dblp_a") && cor.getSecondRecord().getIdentifier().contains("acm")){
						correspondencesTermFrequencies.add(cor);
						
					}
					
					else if(cor.getFirstRecord().getIdentifier().contains("dblp_b") && cor.getSecondRecord().getIdentifier().contains("scholar")){
						correspondencesTermFrequencies.add(cor);
					}
					}
				}
			}
			
			
			MatchingEvaluator<MatchableTableRow, MatchableValue> evaluator = new MatchingEvaluator<MatchableTableRow, MatchableValue>();
			Performance perfTestPairSelection = evaluator.evaluateMatching(correspondencesTermFrequencies.get(),
					gsTrue);

			// print the evaluation result
			logger.info(String.format("Knowledge Base %s <-> %s", knowledgeBaseClass, webTable.getPath()));
			logger.info(String.format("Precision: %.4f", perfTestPairSelection.getPrecision()));
			logger.info(String.format("Recall: %.4f", perfTestPairSelection.getRecall()));
			logger.info(String.format("F1: %.4f", perfTestPairSelection.getF1()));
			
			
			// write the correspondences to the output file
			try {
				new CSVCorrespondenceFormatter().writeCSV(new File(
						String.format("data/%s/output/indexingStrategy/knowledgebase_%s_2_%s_correspondences_label.csv",
								knowledgeBaseClass, knowledgeBaseClass, webTable.getPath().replace(".csv", ""))),
						correspondencesTermFrequencies);
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			}
		}

	}

	public Collection<ExperimentLogger> getResult() {
		return this.experiments;
	}
}
