package de.uni_mannheim.informatik.dws.GMKT.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.logging.log4j.Logger;

import au.com.bytecode.opencsv.CSVWriter;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.FeatureVectorDataSet;
import de.uni_mannheim.informatik.dws.winter.utils.WinterLogManager;
import weka.classifiers.functions.SimpleLogistic;
import weka.classifiers.trees.lmt.LogisticBase;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;

public class SimpleLogisticInspector {

	private static final Logger logger = WinterLogManager.getLogger();

	public static void inspectDecisionTree(SimpleLogistic cls, String sourceWebPath, String modelImportancePath) {

		// display classifier
		for (String row : cls.toString().split("\n")) {
			logger.info(row);
		}

		/*
		 * // Calculate Probability per Feature Map<String, Double>
		 * probabilityFeatureMap = getImportanceMap(cls, false); String[]
		 * experimentHeaderFeatureProp = { "Rank", "Feature", "Importance" };
		 * 
		 * if (modelImportancePath != null) { String modelFeatureImportancePath
		 * = modelImportancePath.replace("PLACEHOLDER", "Feature");
		 * writeImportanceToFile(modelFeatureImportancePath,
		 * probabilityFeatureMap, experimentHeaderFeatureProp); }
		 * 
		 * 
		 * // Calculate Probability per Attribute Map<String, Double>
		 * probabilityAttributeMap = getImportanceMap(cls, true); String[]
		 * experimentHeaderAttributeProp = { "Rank", "Attribute", "Importance"
		 * };
		 * 
		 * if (modelImportancePath != null) { String modelFeatureImportancePath
		 * = modelImportancePath.replace("PLACEHOLDER", "Attribute");
		 * writeImportanceToFile(modelFeatureImportancePath,
		 * probabilityAttributeMap, experimentHeaderAttributeProp); }
		 */

		// Calculate Odds per Feature
		Map<String, Double> oddsRatioFeatureMap = getOddsRatio(cls, false, false, false);
		String[] experimentHeaderFeature = { "Rank", "Feature", "Odds" };

		if (modelImportancePath != null) {
			String modelFeatureImportancePath = modelImportancePath.replace("PLACEHOLDER", "Feature_Odds");
			writeImportanceToFile(modelFeatureImportancePath, oddsRatioFeatureMap, experimentHeaderFeature);
		}

		// Calculate Odds per Attribute
		Map<String, Double> oddsRatioAttributeMap = getOddsRatio(cls, true, false, false);
		String[] experimentHeaderAttribute = { "Rank", "Attribute", "Odds" };

		if (modelImportancePath != null) {
			String modelFeatureImportancePath = modelImportancePath.replace("PLACEHOLDER", "Attribute_Odds");
			writeImportanceToFile(modelFeatureImportancePath, oddsRatioAttributeMap, experimentHeaderAttribute);
		}
		
		// Calculate Odds per Attribute + Similarity Measure
		Map<String, Double> oddsRatioAttributSimilarityMeasureMap = getOddsRatio(cls, false, false, true);
		String[] experimentHeaderAttributeSimilarityMeasure = { "Rank", "AttributeSimilarityMeasure", "Odds" };

		if (modelImportancePath != null) {
			String modelFeatureImportancePath = modelImportancePath.replace("PLACEHOLDER", "Attribute_SimilarityMeasure_Odds");
			writeImportanceToFile(modelFeatureImportancePath, oddsRatioAttributSimilarityMeasureMap, experimentHeaderAttributeSimilarityMeasure);
		}
		
		// Calculate Odds per Threshold
		Map<String, Double> oddsRatioFeatureThresholdMap = getOddsRatio(cls, false, true, false);
		String[] experimentHeaderFeatureThreshold = { "Rank", "FeatureThreshold", "Odds" };

		if (modelImportancePath != null) {
			String modelFeatureImportancePath = modelImportancePath.replace("PLACEHOLDER", "FeatureThreshold_Odds");
			writeImportanceToFile(modelFeatureImportancePath, oddsRatioFeatureThresholdMap, experimentHeaderFeatureThreshold);
		}

		/*
		 * // Calculate Gain Ratio Importance per Split Threshold Map<String,
		 * Double> featureThresholdMap = getFeatureThresholdMap(cls); String[]
		 * experimentHeaderSplitThreshold = { "Rank", "Feature", "Threshold" };
		 * 
		 * if (modelImportancePath != null) { String modelFeatureImportancePath
		 * = modelImportancePath.replace("PLACEHOLDER", "Threshold");
		 * writeImportanceToFile(modelFeatureImportancePath,
		 * featureThresholdMap, experimentHeaderSplitThreshold); }
		 */
	}

	private static Map<String, Double> getFeatureThresholdMap(SimpleLogistic cls) {
		Map<String, Double> probabilityIncreaseMap = new HashMap<String, Double>();

		Field fieldBoostesModel;
		try {
			fieldBoostesModel = cls.getClass().getDeclaredField("m_boostedModel");
			fieldBoostesModel.setAccessible(true);
			LogisticBase logBase = (LogisticBase) fieldBoostesModel.get(cls);

			Field fieldTrain = logBase.getClass().getDeclaredField("m_train");
			fieldTrain.setAccessible(true);
			Instances train = (Instances) fieldTrain.get(logBase);

			Method getCoefficients = logBase.getClass().getDeclaredMethod("getCoefficients");
			getCoefficients.setAccessible(true);
			double[][] coefficients = (double[][]) getCoefficients.invoke(logBase);
			double[] coefficients_match_class = coefficients[1];
			double intercept = 0;

			for (int i = 0; i < coefficients_match_class.length; i++) {

				if (i == 0) {
					intercept = coefficients_match_class[i];
				} else if (Math.abs(intercept) < Math.abs(coefficients_match_class[i])) {
					double threshold = (intercept / coefficients_match_class[i]) * -1;
					Attribute att = train.attribute(i - 1);
					probabilityIncreaseMap.put(att.name(), threshold);
				}
			}

		} catch (NoSuchFieldException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return probabilityIncreaseMap;
	}

	private static Map<String, Double> getImportanceMap(SimpleLogistic cls, boolean attributeAggregation) {
		Map<String, Double> importanceMap = new HashMap<String, Double>();

		Field fieldBoostesModel;
		try {
			fieldBoostesModel = cls.getClass().getDeclaredField("m_boostedModel");
			fieldBoostesModel.setAccessible(true);
			LogisticBase logBase = (LogisticBase) fieldBoostesModel.get(cls);

			Field fieldTrain = logBase.getClass().getDeclaredField("m_train");
			fieldTrain.setAccessible(true);
			Instances train = (Instances) fieldTrain.get(logBase);

			Method getCoefficients = logBase.getClass().getDeclaredMethod("getCoefficients");
			getCoefficients.setAccessible(true);
			double[][] coefficients = (double[][]) getCoefficients.invoke(logBase);
			double[] coefficients_match_class = coefficients[1];
			double valueRange = 0;

			for (int i = 0; i < coefficients_match_class.length; i++) {

				if (i == 0) {
					valueRange = coefficients_match_class[i] * 2;
				} else if (coefficients_match_class[i] != 0) {
					if (!attributeAggregation) {
						double threshold = (coefficients_match_class[i] / valueRange) * -1;
						Attribute att = train.attribute(i - 1);
						importanceMap.put(att.name(), threshold);
					} else {
						Attribute att = train.attribute(i - 1);
						String[] values = att.name().split("~");
						if (values.length > 1) {
							String key = values[1].split("-")[0];

							double coefficientPerAttribute = 0;
							if (importanceMap.containsKey(key)) {
								coefficientPerAttribute = importanceMap.get(key);
							}

							coefficientPerAttribute += (coefficients_match_class[i] / valueRange);
							importanceMap.put(key, coefficientPerAttribute);
						}
					}
				}
			}

		} catch (NoSuchFieldException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return importanceMap;
	}

	public static Map<String, Double> getOddsRatio(SimpleLogistic cls, boolean attributeAggregation,
			boolean thresholdAggregation, boolean attributeSimilarityMeasure) {
		Map<String, Double> probabilityIncreaseMap = new HashMap<String, Double>();

		try {

			Field fieldBoostesModel = cls.getClass().getDeclaredField("m_boostedModel");
			fieldBoostesModel.setAccessible(true);
			LogisticBase logBase = (LogisticBase) fieldBoostesModel.get(cls);

			Field fieldTrain = logBase.getClass().getDeclaredField("m_train");
			fieldTrain.setAccessible(true);
			Instances train = (Instances) fieldTrain.get(logBase);

			double[] initialValues = new double[train.numAttributes()];

			Instance testInstance = new DenseInstance(1.0, initialValues);

			ArrayList<Attribute> attributes = new ArrayList<Attribute>();
			Enumeration<Attribute> enumAtt = train.enumerateAttributes();

			while (enumAtt.hasMoreElements()) {
				attributes.add(enumAtt.nextElement());
			}

			ArrayList<String> labels = new ArrayList<String>();
			labels.add("1");
			labels.add("0");
			Attribute labelAttribute = new Attribute(FeatureVectorDataSet.ATTRIBUTE_LABEL.getIdentifier(), labels);
			attributes.add(labelAttribute);

			Instances dataset = new Instances("testInspection", attributes, 0);
			dataset.add(testInstance);
			dataset.setClassIndex(attributes.size() - 1);

			try {
				double[] distribution = cls.distributionForInstance(dataset.firstInstance());
				int positiveClassIndex = dataset.attribute(dataset.classIndex()).indexOfValue("1");
				double basicProbability = distribution[positiveClassIndex];
				double basicOdds = basicProbability / (1 - basicProbability);

				int[] usedAttributes = logBase.getUsedAttributes()[1];
				int counter = 1;

				if (attributeAggregation || thresholdAggregation || attributeSimilarityMeasure) {
					Map<String, LinkedList<Integer>> attributeFeatureMap = new HashMap<String, LinkedList<Integer>>();
					for (int usedAttribute : usedAttributes) {
						Attribute usedAtt = dataset.attribute(usedAttribute);

						if (attributeAggregation) {
							String[] values = usedAtt.name().split("~");
							if (values.length > 1) {
								String key = values[1].split("-")[0];

								LinkedList<Integer> usedFeaturesPerAttribute = null;
								if (attributeFeatureMap.containsKey(key)) {
									usedFeaturesPerAttribute = attributeFeatureMap.get(key);
								} else {
									usedFeaturesPerAttribute = new LinkedList<Integer>();
								}

								usedFeaturesPerAttribute.add(usedAttribute);
								attributeFeatureMap.put(key, usedFeaturesPerAttribute);

							}
						}

						if (thresholdAggregation && usedAtt.name().contains("-T:")) {
							String[] values = usedAtt.name().split(" ");
							if(values.length > 0){
								String key = values[1].split("/")[0];

								LinkedList<Integer> usedFeaturesPerAttribute = null;
								if (attributeFeatureMap.containsKey(key)) {
									usedFeaturesPerAttribute = attributeFeatureMap.get(key);
								} else {
									usedFeaturesPerAttribute = new LinkedList<Integer>();
								}

								usedFeaturesPerAttribute.add(usedAttribute);
								attributeFeatureMap.put(key, usedFeaturesPerAttribute);
							}
						}
						
						if (attributeSimilarityMeasure) {
							String[] values = usedAtt.name().split(" ");
							if(values.length > 0){
								String key = values[1].split("-")[0];

								LinkedList<Integer> usedFeaturesPerAttribute = null;
								if (attributeFeatureMap.containsKey(key)) {
									usedFeaturesPerAttribute = attributeFeatureMap.get(key);
								} else {
									usedFeaturesPerAttribute = new LinkedList<Integer>();
								}

								usedFeaturesPerAttribute.add(usedAttribute);
								attributeFeatureMap.put(key, usedFeaturesPerAttribute);
							}
						}
					}

					
					for (String attributeName : attributeFeatureMap.keySet()) {
						double[] newValues = initialValues.clone();
						LinkedList<Integer> usedFeaturesPerAttribute = attributeFeatureMap.get(attributeName);
						Iterator<Integer> iterAttIndices = usedFeaturesPerAttribute.iterator();

						while (iterAttIndices.hasNext()) {
							int attIndex = iterAttIndices.next();
							newValues[attIndex] = 1.0;
						}

						testInstance = new DenseInstance(1.0, newValues);
						dataset.add(counter, testInstance);

						distribution = cls.distributionForInstance(dataset.get(counter));
						double advancedProbability = distribution[positiveClassIndex];
						double advancedOdds = advancedProbability / (1 - advancedProbability);
						double odds = advancedOdds / basicOdds;

						probabilityIncreaseMap.put(attributeName, odds);
					}

				} else {
					for (int usedAttribute : usedAttributes) {
						Attribute usedAtt = dataset.attribute(usedAttribute);
						String key = usedAtt.name();

						double[] newValues = initialValues.clone();
						newValues[usedAttribute] = 1.0;
						testInstance = new DenseInstance(1.0, newValues);
						dataset.add(counter, testInstance);

						distribution = cls.distributionForInstance(dataset.get(counter));
						double advancedProbability = distribution[positiveClassIndex];
						double advancedOdds = advancedProbability / (1 - advancedProbability);
						double odds = advancedOdds / basicOdds;

						probabilityIncreaseMap.put(key, odds);

					}
				}

			} catch (Exception e) {
				e.printStackTrace();
				logger.error("Classifier Exception for test Record");
			}

		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (NoSuchFieldException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		/*
		 * if (sumOdds > 0) { for (String name :
		 * probabilityIncreaseMap.keySet()) { double odd =
		 * probabilityIncreaseMap.get(name); double normalizedOdd = odd /
		 * sumOdds;
		 * 
		 * probabilityIncreaseMap.put(name, normalizedOdd); } }
		 */

		return probabilityIncreaseMap;
	}

	public static void writeImportanceToFile(String modelImportancePath, Map<String, Double> gainRatioImportanceMap,
			String[] experimentHeader) {

		if (!gainRatioImportanceMap.isEmpty()) {
			CSVWriter w;
			try {
				w = new CSVWriter(new FileWriter(new File(modelImportancePath), false));

				// Write Header
				w.writeNext(experimentHeader);

				// Sort gainRatioImportanceMap by gainRatioImportance
				int rank = 1;
				Iterator<Entry<String, Double>> iterGainRatioImportance = gainRatioImportanceMap.entrySet().stream()
						.sorted(Map.Entry.<String, Double>comparingByValue().reversed()).iterator();

				// Write values to file
				while (iterGainRatioImportance.hasNext()) {
					Entry<String, Double> entry = iterGainRatioImportance.next();
					String[] experimentRow = { Integer.toString(rank), entry.getKey(),
							Double.toString(entry.getValue()) };
					w.writeNext(experimentRow);
					rank++;
				}
				w.close();
				logger.info("Experiment results written to file: " + modelImportancePath);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
