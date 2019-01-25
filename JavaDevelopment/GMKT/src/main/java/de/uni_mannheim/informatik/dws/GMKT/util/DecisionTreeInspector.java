package de.uni_mannheim.informatik.dws.GMKT.util;

import java.awt.BorderLayout;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.logging.log4j.Logger;

import au.com.bytecode.opencsv.CSVWriter;
import de.uni_mannheim.informatik.dws.winter.utils.WinterLogManager;
import weka.classifiers.trees.J48;

import weka.classifiers.trees.j48.C45Split;
import weka.classifiers.trees.j48.ClassifierSplitModel;
import weka.classifiers.trees.j48.ClassifierTree;
import weka.gui.treevisualizer.PlaceNode2;
import weka.gui.treevisualizer.TreeVisualizer;

public class DecisionTreeInspector {

	private static final Logger logger = WinterLogManager.getLogger();

	public static void inspectDecisionTree(J48 cls, String sourceWebPath, String modelImportancePath) {

		// display classifier
		final javax.swing.JFrame jf = new javax.swing.JFrame(
				String.format("Weka Classifier Tree Visualizer: J48 - %s", sourceWebPath));
		jf.setSize(1500, 1200);
		jf.getContentPane().setLayout(new BorderLayout());
		TreeVisualizer tv = null;
		try {
			tv = new TreeVisualizer(null, cls.graph(), new PlaceNode2());
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		jf.getContentPane().add(tv, BorderLayout.CENTER);
		jf.addWindowListener(new java.awt.event.WindowAdapter() {
			public void windowClosing(java.awt.event.WindowEvent e) {
				jf.dispose();
			}
		});

		jf.setVisible(true);
		tv.fitToScreen();

		// Calculate Gain Ratio Importance per Feature
		Map<String, Double> gainRatioFeatureImportanceMap = getGainRatioImportance(cls, false, false, false, false);
		String[] experimentHeaderFeature = { "Rank", "Feature", "GainRatioImportance" };

		if (modelImportancePath != null) {
			String modelFeatureImportancePath = modelImportancePath.replace("PLACEHOLDER", "Feature");
			writeImportanceToFile(modelFeatureImportancePath, gainRatioFeatureImportanceMap, experimentHeaderFeature);
		}

		// Calculate Gain Ratio Importance per Attribute
		Map<String, Double> gainRatioAttributeImportanceMap = getGainRatioImportance(cls, true, false, false, false);
		String[] experimentHeaderAttribute = { "Rank", "Attribute", "GainRatioImportance" };

		if (modelImportancePath != null) {
			String modelFeatureImportancePath = modelImportancePath.replace("PLACEHOLDER", "Attribute");
			writeImportanceToFile(modelFeatureImportancePath, gainRatioAttributeImportanceMap,
					experimentHeaderAttribute);
		}

		// Calculate Gain Ratio Importance per Feature Threshold
		Map<String, Double> gainRatioFeatureThresholdImportanceMap = getGainRatioImportance(cls, false, false, true, false);
		String[] experimentHeaderFeatureThreshold = { "Rank", "FeatureThreshold", "GainRatioImportance" };

		if (modelImportancePath != null) {
			String modelFeatureImportancePath = modelImportancePath.replace("PLACEHOLDER", "FeatureThreshold");
			writeImportanceToFile(modelFeatureImportancePath, gainRatioFeatureThresholdImportanceMap,
					experimentHeaderFeatureThreshold);
		}

		// Calculate Gain Ratio Importance per Split Threshold
		Map<String, Double> gainRatioSplitImportanceMap = getGainRatioImportance(cls, false, true, false, false);
		String[] experimentHeaderSplitThreshold = { "Rank", "SplitThreshold", "GainRatioImportance" };

		if (modelImportancePath != null) {
			String modelFeatureImportancePath = modelImportancePath.replace("PLACEHOLDER", "SplitThreshold");
			writeImportanceToFile(modelFeatureImportancePath, gainRatioSplitImportanceMap,
					experimentHeaderSplitThreshold);
		}
		
		// Calculate Gain Ratio Importance per Attribute + Similarity Measure
		Map<String, Double> gainRatioAttributeSimilarityImportanceMap = getGainRatioImportance(cls, false, false, false, true);
		String[] experimentHeaderAttributeSimilarityMeasure = { "Rank", "AttributeSimilarityMeasure", "GainRatioImportance" };

		if (modelImportancePath != null) {
			String modelFeatureImportancePath = modelImportancePath.replace("PLACEHOLDER", "AttributeSimilarityMeasure");
			writeImportanceToFile(modelFeatureImportancePath, gainRatioAttributeSimilarityImportanceMap,
					experimentHeaderAttributeSimilarityMeasure);
		}
		

	}

	public static Map<String, Double> getGainRatioImportance(J48 cls, boolean attributeAggregation,
			boolean splitAggregation, boolean featureThreshold, boolean attributeSimilarityMeasure) {
		Map<String, Double> gainRatioImportanceMap = new HashMap<String, Double>();

		try {

			Field fieldSplit = cls.getClass().getDeclaredField("m_root");
			fieldSplit.setAccessible(true);
			ClassifierTree tree = (ClassifierTree) fieldSplit.get(cls);

			C45Split split = (C45Split) tree.getLocalModel();

			Field fieldSumOfWeights = split.getClass().getDeclaredField("m_sumOfWeights");
			fieldSumOfWeights.setAccessible(true);
			double sumInstances = fieldSumOfWeights.getDouble(split);

			gainRatioImportanceMap = updateGainRatioImportance(gainRatioImportanceMap, tree, sumInstances,
					attributeAggregation, splitAggregation, featureThreshold, attributeSimilarityMeasure);

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

		return gainRatioImportanceMap;
	}

	public static Map<String, Double> updateGainRatioImportance(Map<String, Double> gainRatioImportance,
			ClassifierTree tree, double sumInstances, boolean attributeAggregation, boolean splitAggregation,
			boolean featureThreshold, boolean attributeSimilarityMeasure)
			throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {

		ClassifierSplitModel split = tree.getLocalModel();
		if (split.getClass().equals(C45Split.class)) {
			C45Split c45split = (C45Split) split;
			Double gainRatio = c45split.gainRatio();
			Double splitPoint = c45split.splitPoint();

			Field fieldAttIndex = split.getClass().getDeclaredField("m_attIndex");
			fieldAttIndex.setAccessible(true);
			int attIndex = fieldAttIndex.getInt(split);

			Field fieldSumOfWeights = split.getClass().getDeclaredField("m_sumOfWeights");
			fieldSumOfWeights.setAccessible(true);
			double sumOfWeights = fieldSumOfWeights.getDouble(split);

			double gainImportance = 0;
			String key = tree.getTrainingData().attribute(attIndex).name();

			// Extract Attribute Name
			if (attributeAggregation) {
				String[] values = tree.getTrainingData().attribute(attIndex).name().split("~");
				if (values.length > 1) {
					key = values[1].split("-")[0];
				}
			}
			
			// Extract Split Value
			if (splitAggregation) {
				key = String.format("%s~%s", key, Double.toString(splitPoint).substring(0, 3));
			}
			
			// Extract  Attribute Similarity Measure Value
			if (attributeSimilarityMeasure) {
				String[] values = key.split(" ");
				if(values.length > 0){
					key = values[1].split("-")[0];
				}
			}
			
			// Extract  Attribute Similarity Measure Threshold Value
			if (featureThreshold && key.contains("-T:")) {
				String[] values = key.split(" ");
				if(values.length > 0){
					key = values[1].split("/")[0];
				}
			}

			if (gainRatioImportance.containsKey(key)) {
				gainImportance = gainRatioImportance.get(key);
			}
			gainImportance += gainRatio * (sumOfWeights / sumInstances);

			gainRatioImportance.put(key, gainImportance);

			for (ClassifierTree sonTree : tree.getSons()) {
				gainRatioImportance = updateGainRatioImportance(gainRatioImportance, sonTree, sumInstances,
						attributeAggregation, splitAggregation, featureThreshold, attributeSimilarityMeasure);
			}
		}
		return gainRatioImportance;
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
