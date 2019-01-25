package de.uni_mannheim.informatik.dws.GMKT.entitylinking;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import de.uni_mannheim.informatik.dws.GMKT.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.GMKT.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.GMKT.match.data.DataSets;
import de.uni_mannheim.informatik.dws.GMKT.util.Constants;
import de.uni_mannheim.informatik.dws.GMKT.util.InstanceCorrespondenceSet;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.MatchingGoldStandard;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.utils.WinterLogManager;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;

public class SilverstandardGenerator {

	private static final Logger logger = WinterLogManager.getLogger();
	private DataSets kb;
	private DataSets web;
	private String knowledgeBaseClass;
	private MatchingGoldStandard gsTrainAll;
	private MatchingGoldStandard gsTestAll;
	private MatchingGoldStandard gsAllAll;
	private boolean generateTrainData;

	private int negativeSampleScore;

	private boolean writeToFile;

	public SilverstandardGenerator(DataSets kb, DataSets web, String knowledgeBaseClass, boolean writeToBoolean,
			int negativeSampleScore, boolean generateTrainData) {
		this.kb = kb;
		this.web = web;
		this.knowledgeBaseClass = knowledgeBaseClass;
		this.writeToFile = writeToBoolean;
		this.gsTrainAll = new MatchingGoldStandard();
		this.gsTestAll = new MatchingGoldStandard();
		this.gsAllAll = new MatchingGoldStandard();
		this.negativeSampleScore = negativeSampleScore;
		this.generateTrainData = generateTrainData;
	}

	public void run() {
		List<MatchingGoldStandard> trainSilverStandards = new LinkedList<MatchingGoldStandard>();
		List<MatchingGoldStandard> testSilverStandards = new LinkedList<MatchingGoldStandard>();

		for (Table webTable : web.getTables().values()) {

			// Load gold standard
			MatchingGoldStandard gsTrue = new MatchingGoldStandard();
			try {
				gsTrue.loadFromCSVFile(new File(String.format("data/%s/goldstandard/goldstandard_%s_true_%s",
						knowledgeBaseClass, knowledgeBaseClass, webTable.getPath())));

			} catch (IOException e) {
				e.printStackTrace();
			}

			// load correspondences
			InstanceCorrespondenceSet correspondences = new InstanceCorrespondenceSet();
			try {
				correspondences.loadInstanceCorrespondences(
						new File(String.format(
								"data/%s/output/indexingStrategy/knowledgebase_%s_2_%s_correspondences_label.csv",
								this.knowledgeBaseClass, this.knowledgeBaseClass,
								webTable.getPath().replace(".csv", ""))),
						this.kb, this.web, webTable.getPath());
			} catch (IOException e) {
				e.printStackTrace();
			}

			Map<String, MatchingGoldStandard> silverStandards = augmentSilverstandard(gsTrue,
					correspondences.getInstanceCorrespondences(), negativeSampleScore, webTable);

			trainSilverStandards.add(silverStandards.get("train"));
			testSilverStandards.add(silverStandards.get("test"));

			if (writeToFile) {
				try {
					writeSilverStandardToFile(silverStandards, kb, webTable);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		aggregateGoldstandardForAll(trainSilverStandards, this.gsTrainAll);
		aggregateGoldstandardForAll(testSilverStandards, this.gsTestAll);

		if (writeToFile) {
			try {
				writeAllSilverStandardToFile(kb);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private void aggregateGoldstandardForAll(List<MatchingGoldStandard> silverStandards, MatchingGoldStandard gsAll) {
		Iterator<MatchingGoldStandard> iterSilver = silverStandards.iterator();
		int positiveSize = 0;
		int negativeSize = 0;
		while (iterSilver.hasNext()) {
			MatchingGoldStandard test = iterSilver.next();
			if (test.getPositiveExamples().size() < positiveSize || positiveSize == 0) {
				positiveSize = test.getPositiveExamples().size();
			}
			if (test.getNegativeExamples().size() < negativeSize || negativeSize == 0) {
				negativeSize = test.getNegativeExamples().size();
			}
		}

		iterSilver = silverStandards.iterator();
		while (iterSilver.hasNext()) {
			MatchingGoldStandard test = iterSilver.next();
			for (int i = 0; i < positiveSize; i++) {
				gsAll.addPositiveExample(test.getPositiveExamples().get(i));
			}
			for (int i = 0; i < negativeSize; i++) {
				gsAll.addNegativeExample(test.getNegativeExamples().get(i));
			}
		}

	}

	public Map<String, MatchingGoldStandard> augmentSilverstandard(MatchingGoldStandard gsTrue,
			ArrayList<Correspondence<MatchableTableRow, MatchableTableColumn>> correspondences,
			int maxNegativePerPositiveExamples, Table webTable) {

		Map<String, Pair<String, String>> positiveExampleMap = new HashMap<String, Pair<String, String>>();
		Map<String, Pair<String, String>> positiveExampleMapRemaining = new HashMap<String, Pair<String, String>>();

		Map<String, Integer> positiveExampleContainment = new HashMap<String, Integer>();
		Map<String, Double> positiveExampleSimilarity = new HashMap<String, Double>();

		MatchingGoldStandard silverstandardLeft = new MatchingGoldStandard();
		MatchingGoldStandard silverstandardRight = new MatchingGoldStandard();

		// Add positive examples
		for (Pair<String, String> gsPositiveExample : gsTrue.getPositiveExamples()) {
			positiveExampleMap.put(gsPositiveExample.getFirst(), gsPositiveExample);
			// silverstandard.addPositiveExample(gsPositiveExample);
		}

		Collections.sort(correspondences, new Comparator<Correspondence<MatchableTableRow, MatchableTableColumn>>() {
			@Override
			public int compare(Correspondence<MatchableTableRow, MatchableTableColumn> cor1,
					Correspondence<MatchableTableRow, MatchableTableColumn> cor2) {

				return Double.compare(cor1.getSimilarityScore(), cor2.getSimilarityScore());
			}
		});

		// Add negative examples from left hand side
		for (Correspondence<MatchableTableRow, MatchableTableColumn> cor : correspondences) {
			Pair<String, String> gsPositiveExample = positiveExampleMap.get(cor.getFirstRecord().getIdentifier());
			if (gsPositiveExample != null) {
				if (!gsPositiveExample.getSecond().equals(cor.getSecondRecord().getIdentifier())) {

					if (!silverstandardLeft.containsPositive(cor.getFirstRecord().getIdentifier(),
							cor.getSecondRecord().getIdentifier())
							&& !silverstandardLeft.containsNegative(cor.getFirstRecord().getIdentifier(),
									cor.getSecondRecord().getIdentifier())) {

						// Check counter for already contained negative examples
						int negativeCounterPerPositiveExample = 0;

						if (positiveExampleContainment.containsKey(cor.getFirstRecord().getIdentifier())) {
							negativeCounterPerPositiveExample = positiveExampleContainment
									.get(cor.getFirstRecord().getIdentifier());
						}

						// Increase containment
						negativeCounterPerPositiveExample++;

						if (positiveExampleSimilarity.containsKey(cor.getFirstRecord().getIdentifier())) {
							double similarity = positiveExampleSimilarity.get(cor.getFirstRecord().getIdentifier());
							if (cor.getSimilarityScore() <= similarity) {
								// Increase left and right containment
								negativeCounterPerPositiveExample++;

							}
						}

						if (negativeCounterPerPositiveExample < maxNegativePerPositiveExamples) {

							// Add pair to silver standard
							silverstandardLeft.addNegativeExample(new Pair<String, String>(
									cor.getFirstRecord().getIdentifier(), cor.getSecondRecord().getIdentifier()));

							positiveExampleContainment.put(cor.getFirstRecord().getIdentifier(),
									negativeCounterPerPositiveExample);
						}

					}
				} else {
					positiveExampleSimilarity.put(cor.getFirstRecord().getIdentifier(), cor.getSimilarityScore());
				}
			}
		}

		// Add positive examples for which at least one negative example is
		// contained in the GS
		for (String positiveKey : positiveExampleContainment.keySet()) {
			if (positiveExampleMap.containsKey(positiveKey)) {
				silverstandardLeft.addPositiveExample(positiveExampleMap.get(positiveKey));
			}
		}

		// Augment Silverstandard with "unseen" positive examples
		// Add positive examples
		for (Pair<String, String> gsPositiveExample : gsTrue.getPositiveExamples()) {
			if (!positiveExampleContainment.containsKey(gsPositiveExample.getFirst())) {
				positiveExampleMapRemaining.put(gsPositiveExample.getSecond(), gsPositiveExample);
			}
		}

		// Add negative examples from right hand side
		for (Correspondence<MatchableTableRow, MatchableTableColumn> cor : correspondences) {
			Pair<String, String> gsPositiveExample = positiveExampleMapRemaining
					.get(cor.getSecondRecord().getIdentifier());
			if (gsPositiveExample != null) {
				if (!gsPositiveExample.getFirst().equals(cor.getFirstRecord().getIdentifier())) {

					if (!silverstandardLeft.containsPositive(cor.getFirstRecord().getIdentifier(),
							cor.getSecondRecord().getIdentifier())
							&& !silverstandardLeft.containsNegative(cor.getFirstRecord().getIdentifier(),
									cor.getSecondRecord().getIdentifier())
							&& !silverstandardRight.containsPositive(cor.getFirstRecord().getIdentifier(),
									cor.getSecondRecord().getIdentifier())
							&& !silverstandardRight.containsNegative(cor.getFirstRecord().getIdentifier(),
									cor.getSecondRecord().getIdentifier())) {

						// Check counter for already contained negative examples
						int negativeCounterPerPositiveExample = 0;

						if (positiveExampleContainment.containsKey(gsPositiveExample.getFirst())) {
							negativeCounterPerPositiveExample = positiveExampleContainment
									.get(gsPositiveExample.getFirst());
						}

						// Increase containment
						negativeCounterPerPositiveExample++;

						if (positiveExampleSimilarity.containsKey(gsPositiveExample.getFirst())) {
							double similarity = positiveExampleSimilarity.get(gsPositiveExample.getFirst());
							if (cor.getSimilarityScore() <= similarity) {
								// Increase containment
								negativeCounterPerPositiveExample++;
							}
						}

						if (negativeCounterPerPositiveExample < maxNegativePerPositiveExamples) {

							// Add pair to silver standard
							silverstandardRight.addNegativeExample(new Pair<String, String>(
									cor.getFirstRecord().getIdentifier(), cor.getSecondRecord().getIdentifier()));

							// Increase left and right containment
							negativeCounterPerPositiveExample++;

							positiveExampleContainment.put(gsPositiveExample.getFirst(),
									negativeCounterPerPositiveExample);
						}
					}
				} else {
					positiveExampleSimilarity.put(cor.getFirstRecord().getIdentifier(), cor.getSimilarityScore());
				}
			}
		}

		for (String positiveKey : positiveExampleContainment.keySet()) {
			if (positiveExampleMap.containsKey(positiveKey)) {
				Pair<String, String> gsPair = positiveExampleMap.get(positiveKey);
				if (!silverstandardLeft.containsPositive(gsPair.getFirst(), gsPair.getSecond())) {
					silverstandardRight.addPositiveExample(gsPair);
				}
			}
		}

		logger.info(String.format("New Silverstandard for knowledgeBase class %s and webtable %s",
				this.knowledgeBaseClass, webTable.getPath()));

		return splitSilverstandard(silverstandardLeft, silverstandardRight, webTable.getPath());
	}

	public Map<String, MatchingGoldStandard> splitSilverstandard(MatchingGoldStandard silverstandardLeft,
			MatchingGoldStandard silverstandardRight, String webTablePath) {

		MatchingGoldStandard train = new MatchingGoldStandard();
		MatchingGoldStandard test = new MatchingGoldStandard();
		MatchingGoldStandard all = new MatchingGoldStandard();

		// Load Existing Test Data
		MatchingGoldStandard gsTestData = null;
		if (this.generateTrainData) {
			logger.info("Load existing Test GoldStandard for Consolidation Purposes");
			gsTestData = new MatchingGoldStandard();
			try {
				gsTestData.loadFromCSVFile(new File(String.format("data/%s/goldstandard/silverstandard_%s_test_%s.csv",
						knowledgeBaseClass, knowledgeBaseClass, webTablePath.replace(".csv", ""))));

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		// Put negative examples into Map
		List<Pair<String, String>> negativesLeft = silverstandardLeft.getNegativeExamples();

		// Split positive examples: ratio - 60% train - 20% test - 20% valid
		double countPositivesLeft = 0;
		double sizePositivesLeft = silverstandardLeft.getPositiveExamples().size();
		List<Pair<String, String>> positivesLeft = silverstandardLeft.getPositiveExamples();
		double processedLeft = 0;

		Collections.shuffle(positivesLeft);
		for (Pair<String, String> positivePair : positivesLeft) {
			countPositivesLeft++;
			processedLeft = (countPositivesLeft / sizePositivesLeft) * 100;
			if (processedLeft < 80) {
				if (this.generateTrainData) {
					if (!gsTestData.containsPositive(positivePair.getFirst(), positivePair.getSecond())) {
						train.addPositiveExample(positivePair);
					}

				} else {
					train.addPositiveExample(positivePair);
				}

				this.gsAllAll.addPositiveExample(positivePair);
				all.addPositiveExample(positivePair);

				List<Pair<String, String>> tmpNegatives = new LinkedList<>();
				tmpNegatives.addAll(negativesLeft);
				for (Pair<String, String> negativePair : tmpNegatives) {
					if (positivePair.getFirst().equals(negativePair.getFirst())) {
						if (this.generateTrainData) {
							if (!gsTestData.containsPositive(positivePair.getFirst(), positivePair.getSecond())) {
								train.addNegativeExample(negativePair);
							}

						} else {
							train.addNegativeExample(negativePair);
						}

						all.addNegativeExample(negativePair);
						this.gsAllAll.addNegativeExample(negativePair);
						negativesLeft.remove(negativePair);
					}
				}
				/*
				 * } else if (processedLeft < 10) {
				 * valid.addPositiveExample(positivePair);
				 * this.gsAllAll.addPositiveExample(positivePair);
				 * all.addPositiveExample(positivePair);
				 * 
				 * List<Pair<String, String>> tmpNegatives = new LinkedList<>();
				 * tmpNegatives.addAll(negativesLeft); for (Pair<String, String>
				 * negativePair : tmpNegatives) { if
				 * (positivePair.getFirst().equals(negativePair.getFirst())) {
				 * valid.addNegativeExample(negativePair);
				 * all.addNegativeExample(negativePair);
				 * this.gsAllAll.addNegativeExample(negativePair);
				 * negativesLeft.remove(negativePair); } }
				 */
			} else {
				test.addPositiveExample(positivePair);
				this.gsAllAll.addPositiveExample(positivePair);
				all.addPositiveExample(positivePair);

				List<Pair<String, String>> tmpNegatives = new LinkedList<>();
				tmpNegatives.addAll(negativesLeft);
				for (Pair<String, String> negativePair : tmpNegatives) {
					if (positivePair.getFirst().equals(negativePair.getFirst())) {
						test.addNegativeExample(negativePair);
						all.addNegativeExample(negativePair);
						this.gsAllAll.addNegativeExample(negativePair);
						negativesLeft.remove(negativePair);
					}
				}
			}
		}

		// Put negative examples into Map
		List<Pair<String, String>> negativesRight = silverstandardRight.getNegativeExamples();

		// Split positive examples: ratio - 60% train - 20% test - 20% valid
		double countPositivesRight = 0;
		double sizePositivesRight = silverstandardRight.getPositiveExamples().size();
		List<Pair<String, String>> positivesRight = silverstandardRight.getPositiveExamples();
		double processedRight = 0;

		Collections.shuffle(positivesRight);
		for (Pair<String, String> positivePair : positivesRight) {
			countPositivesRight++;
			processedRight = (countPositivesRight / sizePositivesRight) * 100;
			if (processedRight < 80) {
				if (this.generateTrainData) {
					if (!gsTestData.containsPositive(positivePair.getFirst(), positivePair.getSecond())) {
						train.addPositiveExample(positivePair);
					}

				} else {
					train.addPositiveExample(positivePair);
				}
				this.gsAllAll.addPositiveExample(positivePair);
				all.addPositiveExample(positivePair);

				List<Pair<String, String>> tmpNegatives = new LinkedList<>();
				tmpNegatives.addAll(negativesRight);
				for (Pair<String, String> negativePair : tmpNegatives) {
					if (positivePair.getSecond().equals(negativePair.getSecond())) {
						if (this.generateTrainData) {
							if (!gsTestData.containsPositive(positivePair.getFirst(), positivePair.getSecond())) {
								train.addNegativeExample(negativePair);
							}
						} else {
							train.addNegativeExample(negativePair);
						}

						all.addNegativeExample(negativePair);
						this.gsAllAll.addNegativeExample(negativePair);
						negativesRight.remove(negativePair);
					}
				}
				/*
				 * } else if (processedRight < 80) {
				 * valid.addPositiveExample(positivePair);
				 * this.gsAllAll.addPositiveExample(positivePair);
				 * all.addPositiveExample(positivePair);
				 * 
				 * List<Pair<String, String>> tmpNegatives = new LinkedList<>();
				 * tmpNegatives.addAll(negativesRight); for (Pair<String,
				 * String> negativePair : tmpNegatives) { if
				 * (positivePair.getSecond().equals(negativePair.getSecond())) {
				 * valid.addNegativeExample(negativePair);
				 * all.addNegativeExample(negativePair);
				 * this.gsAllAll.addNegativeExample(negativePair);
				 * negativesRight.remove(negativePair); } }
				 */
			} else {
				test.addPositiveExample(positivePair);
				this.gsAllAll.addPositiveExample(positivePair);
				all.addPositiveExample(positivePair);

				List<Pair<String, String>> tmpNegatives = new LinkedList<>();
				tmpNegatives.addAll(negativesRight);
				for (Pair<String, String> negativePair : tmpNegatives) {
					if (positivePair.getSecond().equals(negativePair.getSecond())) {
						test.addNegativeExample(negativePair);
						all.addNegativeExample(negativePair);
						this.gsAllAll.addNegativeExample(negativePair);
						negativesRight.remove(negativePair);
					}
				}
			}
		}

		// Add silverstandards to result
		Map<String, MatchingGoldStandard> splitSilverstandard = new HashMap<String, MatchingGoldStandard>();
		
		if(this.generateTrainData){
			for(Pair<String, String> positivePair : gsTestData.getPositiveExamples()) {
				if(!this.gsAllAll.containsPositive(positivePair.getFirst(), positivePair.getSecond())){
					this.gsAllAll.addPositiveExample(positivePair);
				}
				if(!all.containsPositive(positivePair.getFirst(), positivePair.getSecond())){
					all.addPositiveExample(positivePair);
				}
			}
			
			for(Pair<String, String> negativePair : gsTestData.getNegativeExamples()) {
				if(!this.gsAllAll.containsNegative(negativePair.getFirst(), negativePair.getSecond())){
					this.gsAllAll.addNegativeExample(negativePair);
				}
				if(!all.containsNegative(negativePair.getFirst(), negativePair.getSecond())){
					all.addNegativeExample(negativePair);
				}
			}
		}
		
		
		splitSilverstandard.put("train", train);
		splitSilverstandard.put("test", test);
		splitSilverstandard.put("all", all);

		return splitSilverstandard;

	}

	private void writeSilverStandardToFile(Map<String, MatchingGoldStandard> silverStandards, DataSets kb,
			Table webTable) throws IOException {

		String[] dataSets = new String[] { "train", "test", "all" };

		for (String dataSet : dataSets) {
			if(dataSet.equals("test") && this.generateTrainData){
				continue;
			}
			MatchingGoldStandard silverstandard = silverStandards.get(dataSet);
			silverstandard.writeToCSVFile(new File(String.format("data/%s/goldstandard/silverstandard_%s_%s_%s",
					this.knowledgeBaseClass, this.knowledgeBaseClass, dataSet, webTable.getPath())));

			logger.info(String.format(
					"New %s Silverstandard for knowledgeBase class %s and webtable %s is written to file: ", dataSet,
					this.knowledgeBaseClass, webTable.getPath()));
			logger.info(String.format("data/%s/goldstandard/silverstandard_%s_%s_%s", this.knowledgeBaseClass,
					this.knowledgeBaseClass, dataSet, webTable.getPath()));
			silverstandard.printGSReport();
		}

	}

	private void writeAllSilverStandardToFile(DataSets kb2) throws IOException {
		this.gsTrainAll.writeToCSVFile(new File(String.format("data/%s/goldstandard/silverstandard_%s_train_all.csv",
				this.knowledgeBaseClass, this.knowledgeBaseClass)));

		logger.info(String.format(
				"New train Silverstandard for knowledgeBase class %s and all webtables is written to file: ",
				this.knowledgeBaseClass));
		logger.info(String.format("data/%s/goldstandard/silverstandard_%s_train_%s", this.knowledgeBaseClass,
				this.knowledgeBaseClass, Constants.all));
		this.gsTrainAll.printGSReport();

		if (!this.generateTrainData) {
			this.gsTestAll.writeToCSVFile(new File(String.format("data/%s/goldstandard/silverstandard_%s_test_all.csv",
					this.knowledgeBaseClass, this.knowledgeBaseClass)));
			logger.info(String.format(
					"New test Silverstandard for knowledgeBase class %s and all webtables is written to file: ",
					this.knowledgeBaseClass));
			logger.info(String.format("data/%s/goldstandard/silverstandard_%s_test_%s", this.knowledgeBaseClass,
					this.knowledgeBaseClass, Constants.all));
			this.gsTestAll.printGSReport();
		}

		this.gsAllAll.writeToCSVFile(new File(String.format("data/%s/goldstandard/silverstandard_%s_all_all.csv",
				this.knowledgeBaseClass, this.knowledgeBaseClass)));
		logger.info(String.format(
				"New general Silverstandard for knowledgeBase class %s and all webtables is written to file: ",
				this.knowledgeBaseClass));
		logger.info(String.format("data/%s/goldstandard/silverstandard_%s_all_%s", this.knowledgeBaseClass,
				this.knowledgeBaseClass, Constants.all));
		this.gsAllAll.printGSReport();
	}

}
