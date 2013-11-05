package org.radargun.ml;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.encog.ml.data.MLData;
import org.encog.ml.data.MLDataSet;
import org.encog.ml.data.basic.BasicMLDataSet;
import org.encog.ml.data.specific.CSVNeuralDataSet;
import org.encog.neural.data.NeuralData;
import org.encog.neural.data.basic.BasicNeuralData;
import org.encog.neural.networks.BasicNetwork;
import org.encog.persist.EncogDirectoryPersistence;
import org.encog.util.csv.CSVFormat;
import org.radargun.controller.MLController;
import org.radargun.stressors.MLThreadSample;
/*STRUTTURA FILE CSV SUPPORTATA
 * Ordine dei parametri di ogni record : parametri_di_controllo | parametri_non_di_controllo | output
 * 
 * PARAMETRI DEL FILE EG DELLA RETE NEURALE
 * Per l'N-simo parametro di un input deve essere presente la propriet� maxInputN e minInputN corrispondenti ai massimi e minimi valori non normalizzati per quel parametro
 * Per l'N-simo parametro di un output deve essere presente la propriet� maxOutputN e minOutputN corrispondenti ai massimi e minimi valori non normalizzati per quel parametro
 * Numero parametri di input : countInput
 * Numero parametri di output : countOutput
 * Numero parametri di controllo : numControlParam
 */


public class NNNetworkModule implements NNModule{
	private static Log log = LogFactory.getLog(NNNetworkModule.class);
        private BasicNetwork network=null;
	private final CSVFormat format = new CSVFormat('.', ';');
	private Map<String,String> fieldsRangeMap = new HashMap<String, String>(); 
	private static final  int MAX_NORM = 1;
	private static final int MIN_NORM = 0;
	private int countInput=0;
	private int countOutput=0;
	private int numControlParam;
	private double[][] controlInputPermutations;
	private int[] controlParamWeights;
	
	public NNNetworkModule(){
		
	}
	
	public void loadNetworkFromFile(String filename, String validationFile){
		int maxControlInputPermutations=0;
		System.out.println("Loading network");
		network = (BasicNetwork)EncogDirectoryPersistence.loadObject(new File(filename));
		fieldsRangeMap = network.getProperties();
		this.numControlParam = Integer.parseInt(fieldsRangeMap.get("numControlParam"));
		this.controlParamWeights = new int[numControlParam];
		maxControlInputPermutations = calculateMaxControlInputPermutations();
		this.controlInputPermutations = new double[maxControlInputPermutations][numControlParam];
		fillInputPermutations();
		//da modificare : assegno i pesi che i singoli parametri di controllo hanno nella scelta della miglior configurazione
		for(int i=0 ; i<controlParamWeights.length; i++){
			controlParamWeights[i]=1;
		}
		
		countInput = Integer.parseInt(fieldsRangeMap.get("countInput"));
		countOutput = Integer.parseInt(fieldsRangeMap.get("countOutput"));
		if(validationFile!=null){
			CSVNeuralDataSet ds = new CSVNeuralDataSet(validationFile, network.getInputCount(), network.getOutputCount(), false, format, false);
			double e = network.calculateError(ds);
			System.out.println("Loaded network's error is(should be same as above): "+ e);
		}
		System.out.println("Network loaded");
	}
	
	public double[] calculateOuputFromMLSample(MLThreadSample sample, boolean normalize){
		if(network==null){
			System.out.println("No network loaded yet.");
			return null;
		}
		double[] input_array = sample.toDoubleArray();
		double[] result = new double[countOutput];
		input_array = MLUtil.normalizeInputRecord(input_array, fieldsRangeMap, MAX_NORM , MIN_NORM , countInput);
		MLData input = new BasicNeuralData(input_array);
		MLData output =  network.compute(input);
		
		
		for(int i=0 ; i<countOutput ; i++){
                        String outputMaxRangeKey = "maxOutput" + String.valueOf(i+1);
                        String outputMinRangeKey = "minOutput" + String.valueOf(i+1);
			result[i] = MLUtil.deNormalize(output.getData(i) , Double.parseDouble(fieldsRangeMap.get(outputMaxRangeKey)) , Double.parseDouble(fieldsRangeMap.get(outputMinRangeKey)) ,  MAX_NORM , MIN_NORM);
		}
		return result;
	}
	
	
	//resituisce un array relativo alla prevista configurazione ottimale per lo stato attuale della piattaforma
	public double[] calculateBestOutputConfig(MLThreadSample sample, boolean normalize){
		if(network==null){
			System.out.println("No network loaded yet.");
			return null;
		}
		double[] bestInputParam;
		bestInputParam = getOptimalControlValues(sample);
		
		return bestInputParam;
	}
	
	
	
	private double[] getOptimalControlValues(MLThreadSample sample){
		double[] sampleArray = sample.toDoubleArray();
		double[] optimalControl=null, input_array,input_array_norm;
		double maxInputPerformance=10000,inputPerformance;
		double[] nonControlInput = getNonControlInputArray(sampleArray , numControlParam , countInput); 
		
		
		for(int i=0 ; i<controlInputPermutations.length ; i++){
			input_array = concat(controlInputPermutations[i], nonControlInput);
			
			input_array_norm = MLUtil.normalizeInputRecord(input_array, fieldsRangeMap, MAX_NORM , MIN_NORM, countInput);
			MLData input = new BasicNeuralData(input_array_norm);
			MLData output =  network.compute(input);
			inputPerformance = calculatePerformance(input_array , output.getData());
			
			if(inputPerformance < maxInputPerformance){
				maxInputPerformance = inputPerformance;
				optimalControl = getControlInputArray(input_array , numControlParam);
                                
                                String s="";
                                for(int h=0 ; h<optimalControl.length ; h++)
                                    s = s + output.getData(h) + ",";
                                log.info(s);
                        }
		}
		return optimalControl;
	}
	
	
	private int calculateMaxControlInputPermutations(){
		int total=1;
		for(int i=1 ; i<=this.numControlParam ; i++){
			total = total * ((Integer.parseInt((fieldsRangeMap.get("maxInput" + i))) - (Integer.parseInt(fieldsRangeMap.get("minInput" + i)))) +1 ); 
		}
		return total;
	}
	
	private void fillInputPermutations(){
		int maxNumPermutations = calculateMaxControlInputPermutations();
		
		double[][] widerControlInput = ControlInputArray();
		LinkedHashSet<LinkedList<Double>> s = new LinkedHashSet<LinkedList<Double>>();
		LinkedHashSet<LinkedList<Double>> aux = new LinkedHashSet<LinkedList<Double>>();
		s.add( new LinkedList<Double>());
		aux.add( new LinkedList<Double>());
		int n = 0;
		for(int i=0 ; i<numControlParam ; i++){			
			aux.clear();
			
			Iterator<LinkedList<Double>> it = s.iterator();
			while(it.hasNext()){
				LinkedList<Double> k = it.next();
				for(int j=0 ; j<widerControlInput[0].length ; j++){
					LinkedList<Double> r = (LinkedList<Double>) k.clone();
					if(widerControlInput[i][j] != 0){
						r.add(widerControlInput[i][j]);
						aux.add(r);	
					}	
				}		
			}
			s =(LinkedHashSet<LinkedList<Double>>) aux.clone();	
		}
		
		Iterator<LinkedList<Double>> it = s.iterator();
		int j=0;
		while(it.hasNext()){
			LinkedList<Double> l = it.next();
			controlInputPermutations[j] = linkedListTodoubleArray(l);
			j++;
		}
	}
	
	private double[] linkedListTodoubleArray(LinkedList<Double> l){
		double[] result = new double[numControlParam];
		Iterator<Double> it = l.iterator();
		int i=0;
		while(it.hasNext()){
			Double d = it.next();
			result[i] = d;
			i++;
		}
		return result;
	}
	
	private double[][] ControlInputArray(){
		double[][] auxArray;
		double maxInterval = 0;
		double interval;
		for(int i=1; i<=numControlParam ; i++){
			String outputMaxRangeKey = "maxInput" + i;
			String outputMinRangeKey = "minInput" + i;
			interval = Double.parseDouble(fieldsRangeMap.get(outputMaxRangeKey)) - Double.parseDouble(fieldsRangeMap.get(outputMinRangeKey)) + 1;
			if(interval > maxInterval)
				maxInterval = interval;
		}
		auxArray = new double[numControlParam][(int)maxInterval];
		double offset;
		for(int i=1 ; i<=numControlParam ; i++){
			offset = Double.parseDouble(fieldsRangeMap.get("maxInput" + i)) - Double.parseDouble(fieldsRangeMap.get("minInput" + i)) + 1;
			for(int j=1 ; j<=maxInterval ; j++){
				if(j<=offset){
					auxArray[i-1][j-1] = Double.parseDouble(fieldsRangeMap.get("minInput"+i)) + j -1;
				}else{
					auxArray[i-1][j-1] = 0;
				}
			}
		}
		return auxArray;
	}
	
	private static double[] getNonControlInputArray(double[] sample , int numControlInput , int numInput){
		double[] result = new double[numInput - numControlInput];
		for(int i=0 ; i<result.length ; i++){
			result[i] = sample[numControlInput+i];
		}
		return result;
	}
	
	private static double[] getControlInputArray(double[] sample , int numControlInput){
		double[] result = new double[numControlInput];
		for(int i=0 ; i<numControlInput ; i++){
			result[i] = sample[i];
		}
		return result;
	}
	
	private static double[] concat(double[] A, double[] B) {
		   int aLen = A.length;
		   int bLen = B.length;
		   double[] C= new double[aLen+bLen];
		   System.arraycopy(A, 0, C, 0, aLen);
		   System.arraycopy(B, 0, C, aLen, bLen);
		   return C;
		}
	
	private static double calculatePerformance(double[] input , double[] output){
	    double result=0;
            for(int i=0 ; i<output.length ; i++){
                result = result + output[i];
            }
            return result;
	}
	
}
