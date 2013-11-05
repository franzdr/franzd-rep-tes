package org.radargun.ml;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.radargun.stressors.MLThreadSample;

public class NNTrainingModule implements NNModule{
	
	private List<double[]> tempStorage;
	private String trainingFile;
	
	public NNTrainingModule(String trainingFile){
		this.trainingFile = trainingFile;
                tempStorage = new LinkedList<double[]>();
	}
	
	public void storeMLSample(MLThreadSample sample){
		double[] sampleValues = sample.toDoubleArray();
		tempStorage.add(sampleValues);
	}
	
	public void flushStorageToCSV() throws IOException{
		File f = new File(trainingFile);
		BufferedWriter bw = new BufferedWriter(new FileWriter(f , true));
		Iterator<double[]> it = tempStorage.iterator();
		double[] record;
		String recordToFile;
		while(it.hasNext()){
			record = it.next();
			recordToFile = buildString(record);
			bw.write(recordToFile + "\n");
		}
		bw.close();
	}
	
	private String buildString(double[] record){
		String result="";
		for(int i=0 ; i<record.length ; i++){
			if(i!=record.length-1){
				result +=String.valueOf(record[i])+";";
			}else{
				result +=String.valueOf(record[i]);
			}
		}
		return result;
	}
	
	
}
