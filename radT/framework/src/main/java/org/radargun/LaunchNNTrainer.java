/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.radargun;

import java.io.File;
import java.util.Iterator;
import java.util.List;

import org.encog.ConsoleStatusReportable;
import org.encog.Encog;
import org.encog.engine.network.activation.ActivationSigmoid;
import org.encog.mathutil.randomize.ConsistentRandomizer;
import org.encog.ml.data.MLData;
import org.encog.ml.data.MLDataPair;
import org.encog.ml.data.MLDataSet;
import org.encog.ml.data.basic.BasicMLDataSet;
import org.encog.ml.data.specific.CSVNeuralDataSet;
import org.encog.ml.train.MLTrain;
import org.encog.neural.data.basic.BasicNeuralData;
import org.encog.neural.networks.BasicNetwork;
import org.encog.neural.networks.layers.BasicLayer;
import org.encog.neural.networks.training.propagation.back.Backpropagation;
import org.encog.neural.networks.training.propagation.resilient.ResilientPropagation;
import org.encog.persist.EncogDirectoryPersistence;
import org.encog.util.csv.CSVFormat;
import org.encog.util.normalize.DataNormalization;
import org.encog.util.normalize.input.InputField;
import org.encog.util.normalize.input.InputFieldCSV;
import org.encog.util.normalize.output.OutputFieldRangeMapped;
import org.encog.util.normalize.target.NormalizationStorageCSV;
import org.encog.util.simple.EncogUtility;
 
public class LaunchNNTrainer {
	private static String ML_FOLDER ="mlResources";
        private static String OUTPUT_NETWORK_FILENAME ="trainedNN.eg";
	private static String NORM_SUBFIX = "_normalized.csv";
        private static String normalizedFilePath;
	private static String filePath=null;
	private static double learningRate = 0.1;
	private static double momentum = 0.01;
        private static double error = 0.005;
	private static boolean onlyNormalization = false;
	
	public static void main(final String[] args) {
		parseCommand(args);
		if(filePath == null){
			System.out.println("No Dataset found.");
			System.exit(-1);
		}
		
		try{
			normalize(filePath);
		}catch(Exception e){
			System.out.println("Cannot normalize the Dataset.");
			e.printStackTrace();
                        System.exit(-1);
		}
		if(onlyNormalization)
			System.exit(0);
		// create a neural network, without using a factory
		BasicNetwork network = new BasicNetwork();
		network.addLayer(new BasicLayer(null,true,7));
		network.addLayer(new BasicLayer(new ActivationSigmoid(),true,33));
		network.addLayer(new BasicLayer(new ActivationSigmoid(),false,3));
		network.getStructure().finalizeStructure();
		network.reset();
 

		CSVFormat format = new CSVFormat('.', ';');

		CSVNeuralDataSet trainingSet = new CSVNeuralDataSet(normalizedFilePath, 7, 3, true, format, false);
		
		// train the neural network
		//final ResilientPropagation train = new ResilientPropagation(network, trainingSet);
		final Backpropagation train = new Backpropagation(network, trainingSet);
		train.setLearningRate(learningRate);
		train.setMomentum(momentum);
 
		int epoch = 1;
 
		do {
			train.iteration();
			System.out.println("Epoch #" + epoch + " Error:" + train.getError());
			epoch++;
		} while(train.getError() > error);
		train.finishTraining();
		
		// test the neural network
		System.out.println("Neural Network Results:");
		System.out.println("Error : " + train.getError());
		for(MLDataPair pair: trainingSet ) {
			final MLData output = network.compute(pair.getInput());
				/*System.out.println(pair.getInput().getData(0) + "," + pair.getInput().getData(1)
					+ ", actual=" + output.getData(0) + ",ideal=" + pair.getIdeal().getData(0));*/
			System.out.println(pair.getIdeal().getData(0) + "," + pair.getIdeal().getData(1) + "," + pair.getIdeal().getData(2)
					+ ", actual1=" + output.getData(0) + ", actual2=" + output.getData(1) +  ", actual3=" + output.getData(2));
		}
		
		EncogDirectoryPersistence.saveObject(new File(ML_FOLDER + "/" +OUTPUT_NETWORK_FILENAME), network);
 
		Encog.getInstance().shutdown();
	}
	
	private static void parseCommand(String[] args){
		int i = 0;
		while(i < args.length){
			if(args[i].equals("-file")){
				filePath = args[++i]; 
			}else if(args[i].equals("-lr")){
				learningRate = Double.parseDouble(args[++i]); 
			}else if(args[i].equals("-m")){
				momentum = Double.parseDouble(args[++i]); 
			}else if(args[i].equals("-e")){
				error = Double.parseDouble(args[++i]); 
			}else if(args[i].equals("-norm")){
				onlyNormalization = true;
			}
			i++;
		}
	}
	
	public static void normalize(String filePath) throws Exception{

			File rawFile = new File(filePath);


			// define the format of the iris data

			DataNormalization norm = new DataNormalization();
			InputField nodes, threads, rf, ar, pNO, pP, pOS, dNO, dP, dOS;

			norm.addInputField(nodes = new InputFieldCSV(true,rawFile, 0));
			norm.addInputField(threads = new InputFieldCSV(true,rawFile, 1));
			norm.addInputField(rf = new InputFieldCSV(true,rawFile, 2));
			norm.addInputField(ar = new InputFieldCSV(true,rawFile, 3));
			norm.addInputField(pNO = new InputFieldCSV(true,rawFile, 4));
			norm.addInputField(pP = new InputFieldCSV(true,rawFile, 5));
			norm.addInputField(pOS = new InputFieldCSV(true,rawFile, 6));
			norm.addInputField(dNO = new InputFieldCSV(true,rawFile, 7));
			norm.addInputField(dP = new InputFieldCSV(true,rawFile, 8));
			norm.addInputField(dOS = new InputFieldCSV(true,rawFile, 9));

			
			// define how we should normalize

			norm.addOutputField(new OutputFieldRangeMapped(nodes, 0,1));
			norm.addOutputField(new OutputFieldRangeMapped(threads, 0,1));
			norm.addOutputField(new OutputFieldRangeMapped(rf, 0,1));
			norm.addOutputField(new OutputFieldRangeMapped(ar, 0,1));
			norm.addOutputField(new OutputFieldRangeMapped(pNO, 0,1));
			norm.addOutputField(new OutputFieldRangeMapped(pP, 0,1));
			norm.addOutputField(new OutputFieldRangeMapped(pOS, 0,1));
			norm.addOutputField(new OutputFieldRangeMapped(dNO, 0,1));
			norm.addOutputField(new OutputFieldRangeMapped(dP, 0,1));
			norm.addOutputField(new OutputFieldRangeMapped(dOS, 0,1));


			// define where the output should go
                        String fileName = getFileName(filePath);
                        normalizedFilePath = ML_FOLDER + "/" + fileName + NORM_SUBFIX;
			File outputFile = new File(normalizedFilePath);
			CSVFormat newF = new CSVFormat('.', ';');
			norm.setCSVFormat(newF);
			norm.setTarget(new NormalizationStorageCSV(newF,outputFile));

			// process
			norm.setReport(new ConsoleStatusReportable());
			norm.process();
			System.out.println("Output written to: "+ normalizedFilePath);
			

		
	}
        
        private static String getFileName(String filePath){
            String[] tokens;
            String fileName;
            tokens = filePath.split("/");
            fileName = tokens[tokens.length-1];
            tokens = fileName.split(".csv");
            fileName = tokens[0];
            return fileName;
        }
}