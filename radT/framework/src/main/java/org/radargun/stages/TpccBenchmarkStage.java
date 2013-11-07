package org.radargun.stages;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;
import org.radargun.config.Property;
import org.radargun.config.Stage;
import org.radargun.state.MasterState;
import org.radargun.stressors.Statistics;
import org.radargun.stressors.TpccStressor;

import static java.lang.Double.parseDouble;
import org.radargun.stressors.TpccStressorAlternative;
import static org.radargun.utils.Utils.numberFormat;

/**
 * Simulate the activities found in complex OLTP application environments.
 * Execute the TPC-C Benchmark.
 * <pre>
 * Params:
 *       - numThreads : the number of stressor threads that will work on each slave.
 *       - perThreadSimulTime : total time (in seconds) of simulation for each stressor thread.
 *       - arrivalRate : if the value is greater than 0.0, the "open system" mode is active and the parameter represents the arrival rate (in transactions per second) of a job (a transaction to be executed) to the system; otherwise the "closed system" mode is active: this means that each thread generates and executes a new transaction in an iteration as soon as it has completed the previous iteration.
 *       - paymentWeight : percentage of Payment transactions.
 *       - orderStatusWeight : percentage of Order Status transactions.
 *       - mLSampleTime : Sampling Time.
 *       - numMasterThreads : Number of Master Producer Threads.
 *       - benchmarkMode : BenchmarkMode.
 *       - dynamicTransactions : Dynamic Transactions.
 *       - variableArrivalRate : Variable ArrivalRate.
 * </pre>
 *
 * @author peluso@gsd.inesc-id.pt , peluso@dis.uniroma1.it
 */
@Stage(doc = "Simulate the activities found in complex OLTP application environments.")
public class TpccBenchmarkStage extends AbstractDistStage {
   
   private static final String SIZE_INFO = "SIZE_INFO";
   public static final String SESSION_PREFIX = "SESSION";

   @Property(doc = "Number of threads that will work on this slave. Default is 10.")
   private int numOfThreads = 10;
   
   @Property(doc = "Total time (in seconds) of simulation for each stressor thread. Default is 180.")
   private long perThreadSimulTime = 180L;
   
   @Property(doc = "Average arrival rate of the transactions to the system. Default is 0.")
   private double arrivalRate = 0.0D;
   
   @Property(doc = "Percentage of Payment transactions. Default is 45 %.")
   private double paymentWeight = 45.0D;
   
   @Property(doc = "Percentage of Order Status transactions. Default is 5 %.")
   private double orderStatusWeight = 5.0D;
   
   /**
   * sample time for the ML controller on the master
    */
   @Property(doc = "Sampling Time.")
   private long mLSampleTime =  10L;
   
   @Property(doc = "Number of Master Producer Threads.")
   private int numMasterThreads = 1;
   
   @Property(doc = "BenchmarkMode.")
   private int benchmarkMode = 0;
   
   @Property(doc = "Dynamic Transactions.")
   private int dynamicTransactions = 0;
   
   @Property(doc = "Variable ArrivalRate.")
   private int variableArrivalRate = 0;
   
   
   private TpccStressorAlternative stressorA;

   private CacheWrapper cacheWrapper;

   public DistStageAck executeOnSlave() {
      DefaultDistStageAck result = new DefaultDistStageAck(slaveIndex, slaveState.getLocalAddress());
      this.cacheWrapper = slaveState.getCacheWrapper();
      if (cacheWrapper == null) {
         log.info("Not running test on this slave as the wrapper hasn't been configured.");
         return result;
      }

      log.info("Starting TpccBenchmarkStage: " + this.toString());

          TpccStressorAlternative tpccStressorAlternative = new TpccStressorAlternative();
          tpccStressorAlternative.setNumOfThreads(this.numOfThreads);
          tpccStressorAlternative.setMLSampleTime(this.mLSampleTime);
          tpccStressorAlternative.setNodeIndex(getSlaveIndex());
          stressorA = tpccStressorAlternative;
          try {
              Map<String, String> results = tpccStressorAlternative.stress(cacheWrapper);
              result.setPayload(results);
              return result;
          }catch(Exception e){
             log.warn("Exception while initializing the test", e);
             result.setError(true);
             result.setRemoteException(e);
             return result; 
          }
      
   }

   public boolean processAckOnMaster(List<DistStageAck> acks, MasterState masterState) {
      logDurationInfo(acks);
      boolean success = true;
      Map<Integer, Map<String, Object>> results = new HashMap<Integer, Map<String, Object>>();
      masterState.put(CsvReportGenerationStage.RESULTS, results);
      for (DistStageAck ack : acks) {
         DefaultDistStageAck wAck = (DefaultDistStageAck) ack;
         if (wAck.isError()) {
            success = false;
            log.warn("Received error ack: " + wAck);
         } else {
            if (log.isTraceEnabled())
               log.trace(wAck);
         }
         Map<String, Object> benchResult = (Map<String, Object>) wAck.getPayload();
         if (benchResult != null) {
            results.put(ack.getSlaveIndex(), benchResult);
            Object reqPerSes = benchResult.get(Statistics.REQ_PER_SEC);
            if (reqPerSes == null) {
               throw new IllegalStateException("This should be there!");
            }
            log.info("On slave " + ack.getSlaveIndex() + " we had " + numberFormat(parseDouble(reqPerSes.toString())) + " requests per second");
            log.info("Received " +  benchResult.remove(SIZE_INFO));
         } else {
            log.trace("No report received from slave: " + ack.getSlaveIndex());
         }
      }
      return success;
   }
   
   public TpccStressorAlternative getTpccStressorA(){
       return this.stressorA;
   }

   public void setNumOfThreads(int numOfThreads) {
      this.numOfThreads = numOfThreads;
   }
   
   public int getNumOfThreads() {
      return this.numOfThreads;
   }
   
   public void setConfigName(String configName){
       this.configName = configName;
   }
   
   public String getConfigName(){
       return this.configName;
   }
   
   public void setPerThreadSimulTime(long perThreadSimulTime) {
      this.perThreadSimulTime = perThreadSimulTime;
   }

   public void setArrivalRate(double arrivalRate) {
      this.arrivalRate = arrivalRate;
   }
   
   public void setVariableArrivalRate(int variableAR) {
      if(variableAR != 0 && variableAR > 0)
         this.variableArrivalRate = variableAR;
   }
   
   public int getVariableArrivalRate() {
        return this.variableArrivalRate;
   }
   
   public void setPaymentWeight(double paymentWeight) {
      this.paymentWeight = paymentWeight;
   }

   public void setOrderStatusWeight(double orderStatusWeight) {
      this.orderStatusWeight = orderStatusWeight;
   }

   public void setMLSampleTime(long mLSampleTime){
      this.mLSampleTime = mLSampleTime;
   }
   
   public void setDynamicTransactions(int d){
           this.dynamicTransactions = d;
   }
   
   public int getDynamicTransactions(){
           return this.dynamicTransactions;
   }

   public void setNumMasterThreads(int numMasterThreads){
      this.numMasterThreads = numMasterThreads;
   }
   
   public int getBenchmarkMode(){
       return this.benchmarkMode;
   }
   
   public void setBenchmarkMode(int m){
       this.benchmarkMode = m;
   }
   
   public int getNumMasterThreads(){
       return this.numMasterThreads;
   }
   
   public double getNewOrderWeight(){
       return 100-(this.paymentWeight+this.orderStatusWeight);
   }
   public double getPaymentWeight(){
       return this.paymentWeight;
   }
   public double getOrderStatusWeight(){
       return this.orderStatusWeight;
   }
           
   public double getArrivalRate(){
       return this.arrivalRate;
   }
}
