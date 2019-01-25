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
package de.uni_mannheim.informatik.dws.GMKT.match.cli;

import de.uni_mannheim.informatik.dws.winter.utils.Executable;
import de.uni_mannheim.informatik.dws.winter.utils.WinterLogManager;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;

import org.apache.logging.log4j.Logger;

import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.GMKT.blocking.NegativePairGenerator;
import de.uni_mannheim.informatik.dws.GMKT.entitylinking.BaselineGenerator;
import de.uni_mannheim.informatik.dws.GMKT.entitylinking.SilverstandardGenerator;
import de.uni_mannheim.informatik.dws.GMKT.entitylinking.TransferLearningGenerator;
import de.uni_mannheim.informatik.dws.GMKT.match.data.DataSets;
import de.uni_mannheim.informatik.dws.GMKT.reporting.ExperimentLogger;

/**
 * @author Alexander Brinkmann (albrinkm@mail.uni-mannheim.de)
 *
 */
public class GMKT_All extends Executable {
	
	private static final Logger logger = WinterLogManager.activateLogger("default");
	
    @Parameter(names = "-knowledgeBaseClass", required=true)
    private String knowledgeBaseClass;

    public static void main(String[] args) throws IOException {
    	GMKT_All app = new GMKT_All();

        if(app.parseCommandLine(GMKT_All.class, args)) {
            app.run();
        }
    }

    public void run() throws IOException {
    		// load data
    		logger.info(String.format("Run all experiments for class %s!", knowledgeBaseClass));
    		
    		String knowledgeBaseLocation = String.format("data/%s/knowledgeBase", knowledgeBaseClass);
    		String webtableLocation = String.format("data/%s/webtable", knowledgeBaseClass);
    		
    		double similarityThreshold = 0.7; //CHANGE THIS VALUE AND UNCOMMENT
    		int negativeSampleScore = 3; //FIX VALUE TO 3 --> Focus on influence of indexing threshold - Silver Standard Generation
    		
            DataSets kb = DataSets.loadWebTables(new File(knowledgeBaseLocation), true, true, true, true);
            DataSets web = DataSets.loadWebTables(new File(webtableLocation), true, true, true, true);
           
            
            // KEEEP IN MIND --> GENERATE TEST DATA ONLY WITH THE VERY FIRST RUN OTHERWISE COMMENT PART
            // Negative Pair Selection
            logger.info(String.format("Select negative pairs for testing class %s!", knowledgeBaseClass));
            NegativePairGenerator negativePairGeneratorTest = new NegativePairGenerator(kb,web, knowledgeBaseClass, similarityThreshold);
            negativePairGeneratorTest.run();
            
            // Generate Silver Standard 
            logger.info(String.format("Generate silverstandard for testing class %s!", knowledgeBaseClass));
            SilverstandardGenerator silverstandardGeneratorTest = new SilverstandardGenerator(kb, web, knowledgeBaseClass, true, negativeSampleScore, false);
            silverstandardGeneratorTest.run();
            // KEEEP IN MIND --> GENERATE TEST DATA ONLY WITH THE VERY FIRST RUN OTHERWISE COMMENT PART
            
            //THIS SOLUTION IS A HACK TO TEST ON THE SAME DATA SET- PLEASE REVIEW SILVERSTANDARD CREATION IF YOU PLAN TO SERIOUSLY USE THIS CODING!
            
            if(similarityThreshold != 0.7){
            	// Negative Pair Selection
            	logger.info(String.format("Select negative pairs for class %s!", knowledgeBaseClass));
            	NegativePairGenerator negativePairGeneratorTrain = new NegativePairGenerator(kb,web, knowledgeBaseClass, similarityThreshold);
            	negativePairGeneratorTrain.run();
            
            	// Generate Silver Standard 
            	logger.info(String.format("Generate silverstandard for class %s!", knowledgeBaseClass));
            	SilverstandardGenerator silverstandardGeneratorTrain = new SilverstandardGenerator(kb, web, knowledgeBaseClass, true, negativeSampleScore, true);
            	silverstandardGeneratorTrain.run();
            }
            
            
           
            // Generate Baseline
            logger.info(String.format("Generate baseline for class %s!", knowledgeBaseClass));
            BaselineGenerator baselineGenerator = new BaselineGenerator(kb,web, knowledgeBaseClass);
            baselineGenerator.run();
            
            //Define classifier - J48
    		String parametersJ48[] = new String[1];
    		parametersJ48[0] = ""; 
    		String classifierNameJ48 = "J48"; // new instance of tree
            
            // Apply transfer learning
    		logger.info(String.format("Apply transfer learning for class %s!", knowledgeBaseClass));
            TransferLearningGenerator transferLearningGeneratorJ48 = new TransferLearningGenerator(kb,web, knowledgeBaseClass, classifierNameJ48, parametersJ48, false);
            transferLearningGeneratorJ48.run();
            
          //Define classifier - Simple Logistic
    		String parametersLog[] = new String[1];
    		parametersLog[0] = ""; 
    		String classifierNameLog = "SimpleLogistic"; // new instance of tree
            
            // Apply transfer learning
    		logger.info(String.format("Apply transfer learning for class %s!", knowledgeBaseClass));
            TransferLearningGenerator transferLearningGeneratorLog = new TransferLearningGenerator(kb,web, knowledgeBaseClass, classifierNameLog, parametersLog, false);
            transferLearningGeneratorLog.run();
            
            
            // Log results
            LocalDateTime now = LocalDateTime.now();
            ExperimentLogger.writeResultsToFile(baselineGenerator.getResult(), knowledgeBaseClass, similarityThreshold, negativeSampleScore,  now.toString().replaceAll(":", "-"), false);
            ExperimentLogger.writeResultsToFile(transferLearningGeneratorJ48.getResult(), knowledgeBaseClass, similarityThreshold, negativeSampleScore, now.toString().replaceAll(":", "-"), true);
            ExperimentLogger.writeResultsToFile(transferLearningGeneratorLog.getResult(), knowledgeBaseClass, similarityThreshold, negativeSampleScore, now.toString().replaceAll(":", "-"), true);
            
        }
   

}