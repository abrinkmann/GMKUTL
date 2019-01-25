package de.uni_mannheim.informatik.dws.GMKT.reporting;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;

import org.apache.logging.log4j.Logger;

import au.com.bytecode.opencsv.CSVWriter;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Record;
import de.uni_mannheim.informatik.dws.winter.utils.WinterLogManager;

public class ExperimentLogger extends Record implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger logger = WinterLogManager.getLogger();
	
	public ExperimentLogger(String identifier, LocalDateTime now) {
		super(identifier);
		this.setValue(EXPERIMENT, identifier);
		this.setValue(TIMESTAMP, now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss-SSS")));
	}

	public static final Attribute TIMESTAMP = new Attribute("timestamp");
	public static final Attribute EXPERIMENT = new Attribute("experiment");
	public static final Attribute KNOWLEDGEBASECLASS = new Attribute("knowledgebaseClass");
	public static final Attribute WEBTABLE = new Attribute("webtable");
	public static final Attribute TRAINEDONWEBTABLE = new Attribute("trainedOnWebtable");
	public static final Attribute PRECISION = new Attribute("precision");
	public static final Attribute RECALL = new Attribute("recall");
	public static final Attribute F1 = new Attribute("f1");
	public static final Attribute INDEXINGSTRATEGY = new Attribute("indexingStrategy");
	public static final Attribute BLOCKINGKEYGENERATOR = new Attribute("blockingKeyGenerator");
	public static final Attribute TRAINEDMODEL = new Attribute("trainedModel");

	public static final Attribute[] EXPERIMENTLOG = { TIMESTAMP, EXPERIMENT, KNOWLEDGEBASECLASS, WEBTABLE,
			TRAINEDONWEBTABLE, PRECISION, RECALL, F1, INDEXINGSTRATEGY, TRAINEDMODEL };

	@Override
	public boolean hasValue(Attribute attribute) {
		return getValue(attribute) != null;
	}

	@Override
	public String toString() {
		return String.format("[Experiment Log: %s / %s / %s  / %s / %s / %s / %s / %s / %s / %s / %s ]",
				getValue(TIMESTAMP), getValue(EXPERIMENT), getValue(KNOWLEDGEBASECLASS), getValue(WEBTABLE),
				getValue(TRAINEDONWEBTABLE), getValue(PRECISION), getValue(RECALL), getValue(F1),
				getValue(INDEXINGSTRATEGY), getValue(BLOCKINGKEYGENERATOR), getValue(TRAINEDMODEL));
	}

	public static void writeResultsToFile(Collection<ExperimentLogger> experimentLogs, String knowledgeBaseClass, Double similarityThresholdNegativePairSelection, int negativeSampleScore,
			String fileExtension, boolean append) throws IOException {
		
		int threshold = (int) (similarityThresholdNegativePairSelection * 100);
		
		CSVWriter w = new CSVWriter(new FileWriter(
				new File(String.format("logs/%s/experiments/experiments_similarityScore_%d_negativeSamples_%d_%s.csv", knowledgeBaseClass, threshold, negativeSampleScore, fileExtension)),
				append));

		for (ExperimentLogger experimentLog : experimentLogs) {
			String[] experimentRow = { experimentLog.getValue(ExperimentLogger.TIMESTAMP),
					experimentLog.getValue(ExperimentLogger.EXPERIMENT),
					experimentLog.getValue(ExperimentLogger.KNOWLEDGEBASECLASS),
					experimentLog.getValue(ExperimentLogger.WEBTABLE),
					experimentLog.getValue(ExperimentLogger.PRECISION), experimentLog.getValue(ExperimentLogger.RECALL),
					experimentLog.getValue(ExperimentLogger.F1),
					experimentLog.getValue(ExperimentLogger.INDEXINGSTRATEGY),
					experimentLog.getValue(ExperimentLogger.BLOCKINGKEYGENERATOR),
					experimentLog.getValue(ExperimentLogger.TRAINEDONWEBTABLE),
					experimentLog.getValue(ExperimentLogger.TRAINEDMODEL) };
			w.writeNext(experimentRow);
		}
		w.close();
		logger.info(String.format("Experiment results written to file: logs/%s/experiments/experiments_similarityScore_%d_negativeSamples_%d_%s.csv", knowledgeBaseClass, threshold, negativeSampleScore, fileExtension));
	}

	public static void writeResultsToFile(Collection<ExperimentLogger> experimentLogs, String knowledgeBaseClass)
			throws IOException {
		CSVWriter w = new CSVWriter(new FileWriter(
				new File(String.format("logs/%s/experiments/experiments.csv", knowledgeBaseClass)), true));

		for (ExperimentLogger experimentLog : experimentLogs) {
			String[] experimentRow = { experimentLog.getValue(ExperimentLogger.TIMESTAMP),
					experimentLog.getValue(ExperimentLogger.EXPERIMENT),
					experimentLog.getValue(ExperimentLogger.KNOWLEDGEBASECLASS),
					experimentLog.getValue(ExperimentLogger.WEBTABLE),
					experimentLog.getValue(ExperimentLogger.PRECISION), experimentLog.getValue(ExperimentLogger.RECALL),
					experimentLog.getValue(ExperimentLogger.F1),
					experimentLog.getValue(ExperimentLogger.INDEXINGSTRATEGY),
					experimentLog.getValue(ExperimentLogger.BLOCKINGKEYGENERATOR),
					experimentLog.getValue(ExperimentLogger.TRAINEDONWEBTABLE),
					experimentLog.getValue(ExperimentLogger.TRAINEDMODEL) };
			w.writeNext(experimentRow);
		}
		w.close();
		
		logger.info(String.format("Experiment results written to file: logs/%s/experiments/experiments.csv", knowledgeBaseClass));
	}
}
