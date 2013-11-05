package org.radargun.ml;

import java.util.Map;

public class MLUtil {
	//MAX_INPUT1
	public static double[] normalizeRecord(double[] input_array, Map<String,String> ranges , int newMax , int newMin, int numInput , int numOutput){
		double[] result = new double[input_array.length];
		int i=0,inputIndex;
		double act_max,act_min;
		while(i<numInput){
			inputIndex = i+1;
			act_max = Double.valueOf(ranges.get("maxInput"+inputIndex));
			act_min = Double.valueOf(ranges.get("minInput"+inputIndex));
			
			result[i] = normalize(input_array[i], act_max , act_min , newMax , newMin);
			i++;
		}	
		
		int j=1;
		while(i<numOutput+numInput){
			act_max = Double.valueOf(ranges.get("maxOutput"+j));
			act_min = Double.valueOf(ranges.get("minOutput"+j));
			
			result[i] = normalize(input_array[i], act_max , act_min , newMax , newMin);
			i++;
			j++;
		}	
		return result;
	}
	
	public static double[] normalizeInputRecord(double[] input_array, Map<String,String> ranges , int newMax , int newMin, int numInput){
		double[] result = new double[numInput];
		int i=0;
		int inputIndex;
		double act_max,act_min;
		while(i<numInput){
			inputIndex = i+1;
			act_max = Double.valueOf(ranges.get("maxInput"+inputIndex));
			act_min = Double.valueOf(ranges.get("minInput"+inputIndex));
			
			result[i] = normalize(input_array[i], act_max , act_min , newMax , newMin);
			i++;
		}		
		return result;
	}
	
	public static double[] deNormalizeInputRecord(double[] input_array, Map<String,String> ranges , int newMax , int newMin, int numInput){
		double[] result = new double[numInput];
		int i=0;
		int inputIndex;
		double act_max,act_min;
		while(i<numInput){
			inputIndex = i+1;
			act_max = Double.valueOf(ranges.get("maxInput"+inputIndex));
			act_min = Double.valueOf(ranges.get("minInput"+inputIndex));
			
			result[i] = deNormalize(input_array[i], act_max , act_min , newMax , newMin);
			i++;
		}		
		return result;
	}
	
	
	private static double normalize(double input, double max , double min , int newMax , int newMin){
		return ((input - min) / (max - min))* (newMax - newMin) + newMin;
	}
	
	public static double deNormalize(double input, double max , double min, int oldMax , int oldMin){
		return ((min - max) * input - oldMax* min + max * oldMin)/ (oldMin - oldMax);
	}

}
