/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.radargun.controller;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.Master;
import org.radargun.SerializationHelper;
import org.radargun.ml.NNModule;
import org.radargun.ml.NNNetworkModule;
import org.radargun.ml.NNTrainingModule;
import org.radargun.stages.TpccBenchmarkStage;
import org.radargun.stressors.MLThreadSample;

/**
 *
 * @author ennio
 */
public class MLController {
    private final int NUM_NODES_POS = 0;
    private final int NUM_THREADS_POS = 1;
    private final int REPL_FACT_POS = 2;
    
    private static Log log = LogFactory.getLog(MLController.class);
    
    private SamplesReceiverThread receiverThread = null;
    private boolean isMLActive=false;
    private boolean isMLTrainingMode=false;
    private boolean isMLNetworkMode=false;
    private String MLfilename=null;
    private NNModule activeModule;
    private Master master;
    private TpccBenchmarkStage benchmarkStagePrototype;
    
    public MLController (Master m){
        master = m;
        log.info("MLController created");
    }
    
    public void setSampleReceiverThread(SocketChannel s){
        receiverThread = new SamplesReceiverThread(s, this , master);
    }
    
    public void startReceiverThread(){
        receiverThread.start();
        log.info("SamplerReceiver avviato");
    }
    
    public void killReceiverThread(){
        receiverThread.stopReceiving();
    }
    
    public boolean isMLActive(){
       return isMLActive;
   }
   
   public boolean isNetworkMode(){
       return isMLNetworkMode;
   }
   
   public boolean isMLTrainingMode(){
       return isMLTrainingMode;
   }
   
   public void setTpccBenchmarkStagePrototype(TpccBenchmarkStage b){
       this.benchmarkStagePrototype = b;
   }
   
   public void setMLNetworkMode(String filename){
       if(filename!=null){
            this.MLfilename = filename;
            this.isMLActive = true;
            this.isMLNetworkMode=true;
            this.isMLTrainingMode=false;
            this.activeModule = new NNNetworkModule();
            ((NNNetworkModule)this.activeModule).loadNetworkFromFile(filename, null);
       }
   }
   
   public void setMLTrainingMode(String filename){
       if(filename!=null){
            this.MLfilename = filename;
            this.isMLActive = true;
            this.isMLNetworkMode=false;
            this.isMLTrainingMode=true;
            this.activeModule = new NNTrainingModule(filename);
       }
   }
   
   public void receiveSampleToElaborate(MLThreadSample s) throws IOException{
       if(isMLNetworkMode){
           NNNetworkModule m = (NNNetworkModule) activeModule;
           double[] bestConfig = m.calculateBestOutputConfig(s, true);
           
           logBestConfig(bestConfig);
           //nel caso fossero aggiunti nuovi slave nel frattempo
           master.getSlave2IndexLock().lock();
           try{
                sendOptimalConfig(bestConfig);
           }finally{
                master.getSlave2IndexLock().unlock();
           }
       }
       if(isMLTrainingMode){
           NNTrainingModule m = (NNTrainingModule) activeModule;
           m.storeMLSample(s);
       }
   }
   
   private void logBestConfig(double[] bestConfig){
       log.info("Best config predicted:");
       log.info("Nodes:" + bestConfig[NUM_NODES_POS]);
       log.info("Threads: " + bestConfig[NUM_THREADS_POS] );
       log.info("Replication Factor:" + bestConfig[REPL_FACT_POS]);
   }
   
   public void closeMLControllerSafely(){
       if(isMLTrainingMode){
           NNTrainingModule m = (NNTrainingModule) activeModule;
           try {
               m.flushStorageToCSV();
           } catch (IOException ex) {
               log.warn("Couldn't find the file where to flush the buffered data");
           }
       }
   }
   
   public void sendOptimalConfig(double[] bestConfig) throws IOException{
       SocketChannel sc;
       byte[] bytes;
       prepareStageToSend(this.benchmarkStagePrototype, bestConfig);
       bytes = SerializationHelper.prepareForSerialization(benchmarkStagePrototype);
       master.clearWriteBuffer();
       for(Entry<SocketChannel,Integer> entry : master.getSlavesChannel().entrySet()){
           sc = entry.getKey();
           master.putInWriteBuffer(sc, ByteBuffer.wrap(bytes));
           
           SelectionKey k = entry.getKey().keyFor(master.getCommunicationSelector());
           k.interestOps(SelectionKey.OP_WRITE);
           master.getCommunicationSelector().wakeup();
       }
   }
   
   public void prepareStageToSend(TpccBenchmarkStage stage, double[] bestConfig){
       stage.setNumOfThreads((int)bestConfig[NUM_THREADS_POS]);
   }
   
   public TpccBenchmarkStage getUpdateTpccBenchmarkPrototype(){
       return this.benchmarkStagePrototype;
   }
   
}
    

