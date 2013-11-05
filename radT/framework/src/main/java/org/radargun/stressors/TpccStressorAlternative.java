/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.radargun.stressors;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.transaction.Transaction;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.SerializationHelper;
import org.radargun.stages.TpccBenchmarkStage;
import org.radargun.stages.TransactionRequest;
import org.radargun.tpcc.ElementNotFoundException;
import org.radargun.tpcc.TpccTerminal;
import org.radargun.tpcc.transaction.NewOrderTransaction;
import org.radargun.tpcc.transaction.OrderStatusTransaction;
import org.radargun.tpcc.transaction.PaymentTransaction;
import org.radargun.tpcc.transaction.TpccTransaction;

/**
 *
 * @author frank
 */
public class TpccStressorAlternative {
    
  private static Log log = LogFactory.getLog(TpccStressor.class);
    
   private int numOfThreads = 10;
   private long mLSampleTime = 10L;
   private int nodeIndex = -1;
   private ExecutorService threadPool;
   
   private Map<Integer , MLThreadSample> mLThreadSamples = new HashMap<Integer, MLThreadSample>();
   private Map<Integer , Lock> threadSamplingLocks = new HashMap<Integer, Lock>();
   private MLThreadSample mLThreadLastIteration = new MLThreadSample();
   private Map<String , Integer> threadNameToId = new HashMap<String, Integer>();
   private Map<Integer, ByteBuffer> threadsBuffers = new HashMap<Integer, ByteBuffer>();
   private AtomicInteger nextFreeThreadId = new AtomicInteger(0);
   private boolean blockTransactionRequests = false;
   
   private CacheWrapper wrapper;
   
   private boolean isStopped = false;
   
   private static int DEFAULT_SIZE_READ_BUFFER = 512;
   
   public TpccStressorAlternative(){
   }
    
   public void setNumOfThreads(int numOfThreads){
       this.numOfThreads = numOfThreads;
   }
          
   public void setMLSampleTime(long mLSampleTime){
       this.mLSampleTime = mLSampleTime;
   }
          
          
   public Map<String, String> stress(CacheWrapper wrapper){
       log.info("Avviato Alternative");
       this.wrapper = wrapper;
        
      threadPool= Executors.newFixedThreadPool(numOfThreads, new ThreadFactory() {
      public Thread newThread(Runnable r) {
         Thread th = new Thread(r);
         String t_sid = (String) th.getName();
         int threadId = nextFreeThreadId.getAndIncrement();
         threadNameToId.put(t_sid, threadId);
         mLThreadSamples.put(threadId ,new MLThreadSample());
         threadsBuffers.put(threadId , ByteBuffer.allocate(DEFAULT_SIZE_READ_BUFFER));
         threadSamplingLocks.put(threadId, new ReentrantLock());
         log.info("Creato thread " + t_sid);
         return th;
      }
   });
       
       SocketChannel reqSocket = null;
       
       if(nodeIndex == 0){ //the sampler is started only on the first slave
           log.info("Avviato Sampler");
           MLSamplerThread sampler = new MLSamplerThread(this.mLSampleTime);
	   sampler.start();
       }       
       
       ServerSocketChannel acceptSocket=null;

       log.info("Ingresso nel ciclo di servizio");
       //resto in attesa di richieste da parte del master
       try{
           acceptSocket = ServerSocketChannel.open();
           acceptSocket.socket().bind(new InetSocketAddress(15000));
       } catch (IOException ex) {
           Logger.getLogger(TpccStressorAlternative.class.getName()).log(Level.SEVERE, null, ex);
       }
                
                
        while(!isStopped){
           if(!this.blockTransactionRequests){
               try{
                    reqSocket = acceptSocket.accept(); 
                    log.debug("Richiesta di transazione ricevuta");
                } catch (IOException ex) {
                    //Logger.getLogger(TpccStressorAlternative.class.getName()).log(Level.SEVERE, null, ex);
                    //log.error("ERRORE SLAVE ACCEPT: ", ex);
                }
                threadPool.execute(new TransactionRunnable(reqSocket));
           }
        }
       
       threadPool.shutdown();
       return null;
   }
   
   public void blockTransactionRequests(){
       this.blockTransactionRequests = true;
   }
   
   public void unBlockTransactionRequests(){
       this.blockTransactionRequests = false;
   }
   
   public void updateConfig(TpccBenchmarkStage stage){
        //updateNumNodes(stage.getActiveSlaveCount());
        updateNumThreads(stage.getNumOfThreads());
    }
   
   private void updateNumThreads(int n){
        if(n!=this.numOfThreads){
            threadPool.shutdown();
            numOfThreads = n;
            cleanThreadsStructures();
            setThreadPool();
            log.info("Thread pool size has been set to " + this.numOfThreads +".");
        }
    }
   
   private void cleanThreadsStructures(){
        threadNameToId.clear();
        mLThreadSamples.clear();
        threadsBuffers.clear();
        threadSamplingLocks.clear();
        mLThreadLastIteration = new MLThreadSample();
    }
    
    private void setThreadPool(){
        this.threadPool= Executors.newFixedThreadPool(numOfThreads, new ThreadFactory() {
        public Thread newThread(Runnable r) {
         Thread th = new Thread(r);
         String t_sid = (String) th.getName();
         //int t_id = Integer.parseInt(t_sid);
         int threadId = nextFreeThreadId.getAndIncrement();
         threadNameToId.put(t_sid, threadId);
         mLThreadSamples.put(threadId ,new MLThreadSample());
         threadsBuffers.put(threadId , ByteBuffer.allocate(DEFAULT_SIZE_READ_BUFFER));
         threadSamplingLocks.put(threadId, new ReentrantLock());
         log.info("Creato thread " + t_sid);
         return th;
      }
   });
    }
   
   
   public class TransactionRunnable implements Runnable{
       MLThreadSample threadSample;
       SocketChannel commSocket;
       ByteBuffer readBuffer;
       int threadId;
       boolean run = true;
       
       public TransactionRunnable(SocketChannel cs){
           commSocket = cs;
       }

       public void run() {
           log.debug("Entro in run, thread :" + threadId);
           threadId = threadNameToId.get(Thread.currentThread().getName());
           readBuffer = threadsBuffers.get(threadId);
           
           try {
            while (run) {             
               int readBytes = commSocket.read(readBuffer);
               if(readBytes==-1){
                   commSocket.close();
                   log.debug("socket chiuso " + threadId);
                   run=false;
               }else{
                   if(readBuffer.limit() >= 4){
                        int expectedSize = readBuffer.getInt(0);
                        if(readBuffer.position() == expectedSize + 4){
                            TransactionRequest req = (TransactionRequest) SerializationHelper.deserialize(readBuffer.array(), 4, expectedSize);
                            this.executeTransaction(req);
                            log.debug("transazione eseguita_" + threadId);
                            readBuffer.clear();
                        }
                   }
               }
           }
           } catch (IOException ex) {
               Logger.getLogger(TpccStressorAlternative.class.getName()).log(Level.SEVERE, null, ex);
           }
       }
       
       
       private void executeTransaction(TransactionRequest r){
           threadSample = mLThreadSamples.get(threadId);
           long commit_start = 0L;
           long end = 0L;
           boolean isReadOnly = false;
           boolean successful = true;
           TpccTransaction transaction=null;
           int reqType = r.getTransactionType();
           if(reqType == TpccTerminal.NEW_ORDER){
               transaction = new NewOrderTransaction();
           }else if(reqType == TpccTerminal.PAYMENT){
               transaction = new PaymentTransaction(nodeIndex);
           }else if(reqType == TpccTerminal.ORDER_STATUS){
               transaction = new OrderStatusTransaction();
           }
           
           isReadOnly = transaction.isReadOnly();
           long start = System.nanoTime();
           long startService = start;
           
           wrapper.startTransaction();

            try {
               transaction.executeTransaction(wrapper);
            } catch (Throwable e) {
               successful = false;
               log.warn(e);
               if (e instanceof ElementNotFoundException) {
                  threadSample.appFailures++;
               }

               if (e instanceof Exception) {
                  e.printStackTrace();
               }
            }

            //here we try to finalize the transaction
            //if any read/write has failed we abort
            boolean measureCommitTime = false;
            try {
               /* In our tests we are interested in the commit time spent for write txs*/
               if (successful && !isReadOnly) {
                  commit_start = System.nanoTime();
                  measureCommitTime = true;
               }

             wrapper.endTransaction(successful);
             
             threadSamplingLocks.get(threadId).lock();
             try{
             if (!successful) {
                  threadSample.failures++;
                  if (!isReadOnly) {
                     threadSample.wrFailures++;
                     if (transaction instanceof NewOrderTransaction) {
                        threadSample.newOrderFailures++;
                     } else if (transaction instanceof PaymentTransaction) {
                        threadSample.paymentFailures++;
                     }

                  } else {
                     if (transaction instanceof OrderStatusTransaction) {
                        threadSample.orderStatusFailures++;
                     }
                     threadSample.rdFailures++;
                  }

               }
            } catch (Throwable rb) {
               log.info(threadId + "Error while committing");

               threadSample.failures++;
               
               
               if (!isReadOnly) {
                  threadSample.wrFailures++;
                  threadSample.nrWrFailuresOnCommit++;
                  if (transaction instanceof NewOrderTransaction) {
                     threadSample.newOrderFailures++;
                     threadSample.newOrderNotAppFailures++;
                  } else if (transaction instanceof PaymentTransaction) {
                     threadSample.paymentFailures++;
                     threadSample.paymentNotAppFailures++;
                  }
               } else {
                  threadSample.rdFailures++;
                  if (transaction instanceof OrderStatusTransaction) {
                     threadSample.orderStatusFailures++;
                     threadSample.orderStatusNotAppFailures++;
                  }
               }
               successful = false;
               log.warn(rb);

            }


            end = System.nanoTime();



            if (!isReadOnly) {
               threadSample.writesDurations += end - start;
               //threadSample.writeServiceTimes += end - startService;
               if (transaction instanceof NewOrderTransaction) {
                  threadSample.newOrderDurations += end - start;
                  //threadSample.newOrderServiceTimes += end - startService;
               } else if (transaction instanceof PaymentTransaction) {
                  threadSample.paymentDurations += end - start;
                  //threadSample.paymentServiceTimes += end - startService;
               }
               if (successful) {
                  threadSample.successful_writesDurations += end - startService;
                  threadSample.writes++;
                  if (transaction instanceof PaymentTransaction) {
                     threadSample.paymentTransactions++;
                  } else if (transaction instanceof NewOrderTransaction) {
                     threadSample.newOrderTransactions++;
                  }
               }
            } else {
               threadSample.readsDurations += end - start;
               //threadSample.readServiceTimes += end - startService;
               if (successful) {
                  threadSample.reads++;
                  threadSample.successful_readsDurations += end - startService;
                  if (transaction instanceof OrderStatusTransaction) {
                     threadSample.orderStatusDurations += end - start; 
                     threadSample.orderStatusTransactions++;
                   }
               }
            }

            if (measureCommitTime) {
               if (successful) {
                  threadSample.successful_commitWriteDurations += end - commit_start;
               } else {
                  threadSample.aborted_commitWriteDurations += end - commit_start;
               }
               threadSample.commitWriteDurations += end - commit_start;
               }

            }finally{
            threadSamplingLocks.get(threadId).unlock();
            }
       }
       
     }
   
   
   public void setNodeIndex(int nodeIndex){
       this.nodeIndex = nodeIndex;
   }
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   private class MLSamplerThread extends Thread{
	private long MLSamplingTimeMilliseconds;
	private volatile boolean executeSampling= true;

	public MLSamplerThread(long mlst){
		this.MLSamplingTimeMilliseconds = mlst*1000;
        }

	public void stopSampling(){
		this.executeSampling = false;
	}

	public void run(){
		ServerSocket ss=null;
		Socket cs=null;
		ObjectInputStream is=null;
		ObjectOutputStream os=null;
		MLThreadSample sample;
		//apertura socket con il master
		log.info("Waiting a connection to start sampling...");
		try{		
			ss = new ServerSocket(5555);
			ss.setSoTimeout(20000);	
                        try{
                            cs = ss.accept();
                        }catch(SocketTimeoutException ste){
                            if(!this.executeSampling)
                                return;
                        }
			log.info("Sampler received a connection from master.");
			os = new ObjectOutputStream(cs.getOutputStream());
			os.flush();
		        log.info("Sampler output stream opened.");
		}		
		catch(IOException e){
			log.warn("Impossible to connect Sampler with Master, the sampling is DISABLED.");
			return ;
		}
		
                
                
		int i=0;
		while(this.executeSampling){
                try {
		    log.info("Sampler entered the sampling phase.");
                    Thread.sleep(this.MLSamplingTimeMilliseconds);
		     log.info("Sampler awakened.");
		    //raccoglie e invia le statistiche al master
		   for (Entry<Integer, Lock> l : threadSamplingLocks.entrySet()) {
                       l.getValue().lock();
                   }
                   try{
                        sample = collectStatistics(mLThreadSamples);
                        //resetto la media per gli MLSampleThread di tutti i thread
                        for (Entry<Integer, MLThreadSample> e : mLThreadSamples.entrySet()) {
                            e.getValue().resetTransactionsAverages();
                        }
                            
                   }finally{         
                        for (Entry<Integer, Lock> l : threadSamplingLocks.entrySet()) {
                            l.getValue().unlock();
                        }
                   }
	   	    log.info(sample.toString());
		    log.info("//////////////////////////////////////");
	 	    log.info("//////////////////////////////////////");
		    log.info("//////////////////////////////////////");
                    os.writeObject(sample);
	            log.info("MLSample sent.");
                } catch (IOException ex) {
                    log.warn("Communication issues on sampler");
                }catch (InterruptedException ex) {
                    log.warn("?");
                }
		 
		}
		if(!this.executeSampling){
	          try{ 
                        os.writeObject(new Integer(-1));
		  	ss.close();
		  	cs.close();
		  	os.close();
	   	  	is.close();
		  }catch(IOException e){
		  	log.warn("Issues closing the sampling socket");
       		  }
		}
	}


	public MLThreadSample collectStatistics(Map<Integer, MLThreadSample> m){
                MLThreadSample result=new MLThreadSample();

                for (Entry<Integer, MLThreadSample> e : m.entrySet()) {
                    result.transactionsAverageIterations += e.getValue().transactionsAverageIterations;
                    result.newOrderTransactionsAverage += e.getValue().newOrderTransactionsAverage;
                    result.paymentTransactionsAverage += e.getValue().paymentTransactionsAverage;
                    result.orderStatusTransactionsAverage += e.getValue().orderStatusTransactionsAverage;
                    
                    
                    //result.duration += e.getValue().getDuration(); //in nanosec
                    result.readsDurations += e.getValue().getReadsDurations(); //in nanosec
                    result.writesDurations += e.getValue().getWritesDurations(); //in nanosec
                    result.newOrderDurations += e.getValue().getNewOrderDurations(); //in nanosec
                    result.paymentDurations += e.getValue().getPaymentDurations(); //in nanosec
                    result.orderStatusDurations +=e.getValue().getOrderStatusDurations();
                    result.successful_writesDurations += e.getValue().getSuccessful_writesDurations(); //in nanosec
                    result.successful_readsDurations += e.getValue().getSuccessful_readsDurations(); //in nanosec

                    result.successful_commitWriteDurations += e.getValue().getSuccessful_commitWriteDurations(); //in nanosec
                    result.aborted_commitWriteDurations += e.getValue().getAborted_commitWriteDurations(); //in nanosec
                    result.commitWriteDurations += e.getValue().getCommitWriteDurations(); //in nanosec;

                    /*result.writeServiceTimes += e.getValue().getWriteServiceTimes();
                    result.readServiceTimes += e.getValue().getReadServiceTimes();
                    result.newOrderServiceTimes += e.getValue().getNewOrderServiceTimes();
                    result.paymentServiceTimes += e.getValue().getPaymentServiceTimes();*/

                    result.reads += e.getValue().getReads();
                    result.writes += e.getValue().getWrites();
                    result.newOrderTransactions += e.getValue().getNewOrderTransactions();
                    result.paymentTransactions += e.getValue().getPaymentTransactions();
                    result.orderStatusTransactions +=e.getValue().getOrderStatusTransactions();
                    
                    result.newOrderWeight = e.getValue().getNewOrderTransactionsWeight();
                    result.paymentWeight = e.getValue().getPaymentTransactionsWeight();
                    result.orderStatusWeight =e.getValue().getOrderStatusTransactionsWeight();
                    
                    
                    result.failures += e.getValue().getFailures();
                    result.rdFailures += e.getValue().getRdFailures();
                    result.wrFailures += e.getValue().getWrFailures();
                    result.nrWrFailuresOnCommit += e.getValue().getNrWrFailuresOnCommit();
                    result.newOrderFailures += e.getValue().getNewOrderFailures();
                    result.paymentFailures += e.getValue().getPaymentFailures();
                    result.appFailures += e.getValue().getAppFailures();

                    /*result.writeInQueueTimes += e.getValue().getWriteInQueueTimes();
                    result.readInQueueTimes += e.getValue().getReadInQueueTimes();
                    result.newOrderInQueueTimes += e.getValue().getNewOrderInQueueTimes();
                    result.paymentInQueueTimes += e.getValue().getPaymentInQueueTimes();
                    result.numWritesDequeued += e.getValue().getNumWritesDequeued();
                    result.numReadsDequeued += e.getValue().getNumReadsDequeued();
                    result.numNewOrderDequeued += e.getValue().getNumNewOrderDequeued();
                    result.numPaymentDequeued += e.getValue().getNumPaymentDequeued();*/
                
                    //mLThreadSamplesLastIteration.put(threadKey , newLastIteration);
                }
                    //to send
                    MLThreadSample toSend = new MLThreadSample();
                    
                    toSend.nodes = wrapper.getNumMembers();
                    toSend.threads = numOfThreads;
                    toSend.replicationF = 1;
                    toSend.replicationProtocol=0;
                    
                    
                    //toSend.transactionsAverageIterations = (int) result.transactionsAverageIterations/numOfThreads;
                    toSend.newOrderTransactionsAverage = result.newOrderTransactionsAverage/numOfThreads;
                    toSend.paymentTransactionsAverage = result.paymentTransactionsAverage/numOfThreads;
                    toSend.orderStatusTransactionsAverage = result.orderStatusTransactionsAverage/numOfThreads;
                    
                    //toSend.duration = result.duration - mLThreadLastIteration.duration; //in nanosec
                    toSend.readsDurations = ((result.readsDurations - mLThreadLastIteration.readsDurations)*toSend.nodes)/1000; //in nanosec
                    toSend.writesDurations = ((result.writesDurations - mLThreadLastIteration.writesDurations)*toSend.nodes)/1000; //in nanosec
                    toSend.newOrderDurations = ((result.newOrderDurations - mLThreadLastIteration.newOrderDurations)*toSend.nodes)/1000; //in nanosec
                    toSend.paymentDurations = ((result.paymentDurations - mLThreadLastIteration.paymentDurations)*toSend.nodes)/1000; //in nanosec
                    toSend.orderStatusDurations = ((result.orderStatusDurations - mLThreadLastIteration.orderStatusDurations)*toSend.nodes)/1000; //in nanosec
                    
                    toSend.newOrderTransactions = (result.newOrderTransactions - mLThreadLastIteration.newOrderTransactions)*toSend.nodes;
                    toSend.paymentTransactions = (result.paymentTransactions - mLThreadLastIteration.paymentTransactions)*toSend.nodes;
                    toSend.orderStatusTransactions = (result.orderStatusTransactions - mLThreadLastIteration.orderStatusTransactions)*toSend.nodes;
                    
                    if(toSend.newOrderTransactions == 0){
                        toSend.singleNewOrderDurations = 0;
                    }else{
                        toSend.singleNewOrderDurations = toSend.newOrderDurations/toSend.newOrderTransactions;  
                    }
                    
                    if(toSend.paymentTransactions == 0){
                        toSend.singlePaymentDurations = 0;
                    }else{
                        toSend.singlePaymentDurations = toSend.paymentDurations/toSend.paymentTransactions; 
                    }
                    
                    if(toSend.orderStatusTransactions == 0){
                        toSend.singleOrderStatusDurations = 0;
                    }else{
                        toSend.singleOrderStatusDurations = toSend.orderStatusDurations/toSend.orderStatusTransactions;      
                     }

                    
                    
                    toSend.successful_writesDurations = ((result.successful_writesDurations - mLThreadLastIteration.successful_writesDurations)*toSend.nodes)/1000; //in nanosec
                    toSend.successful_readsDurations = ((result.successful_readsDurations - mLThreadLastIteration.successful_readsDurations)*toSend.nodes)/1000; //in nanosec

                    toSend.successful_commitWriteDurations = ((result.successful_commitWriteDurations - mLThreadLastIteration.successful_commitWriteDurations)*toSend.nodes)/1000; //in nanosec
                    toSend.aborted_commitWriteDurations = ((result.aborted_commitWriteDurations - mLThreadLastIteration.aborted_commitWriteDurations)*toSend.nodes)/1000; //in nanosec
                    toSend.commitWriteDurations = ((result.commitWriteDurations - mLThreadLastIteration.commitWriteDurations)*toSend.nodes)/1000; //in nanosec;


                    toSend.reads = (result.reads - mLThreadLastIteration.reads)*toSend.nodes;
                    toSend.writes = (result.writes - mLThreadLastIteration.writes)*toSend.nodes;
                    toSend.arrivalRate = (toSend.reads + toSend.writes)/mLSampleTime;
                    
                    
                    int totTrans = toSend.newOrderTransactions + toSend.paymentTransactions + toSend.orderStatusTransactions;
                    if(totTrans==0){
                        toSend.newOrderPerc = 0;
                        toSend.paymentPerc = 0;
                        toSend.orderStatusPerc = 0;
                    }else{
                        toSend.newOrderPerc =  new BigDecimal((((double)toSend.newOrderTransactions)/totTrans)).setScale(2, BigDecimal.ROUND_UP).doubleValue();
                        toSend.paymentPerc = new BigDecimal(((double)toSend.paymentTransactions)/totTrans).setScale(2, BigDecimal.ROUND_UP).doubleValue();
                        //toSend.orderStatusPerc = ((double)toSend.orderStatusTransactions)/totTrans;
                        toSend.orderStatusPerc = new BigDecimal(((double)1) - (toSend.newOrderPerc + toSend.paymentPerc)).setScale(2, BigDecimal.ROUND_UP).doubleValue();
                    }
                    
                    toSend.newOrderWeight = result.getNewOrderTransactionsWeight();
                    toSend.paymentWeight = result.getPaymentTransactionsWeight();
                    toSend.orderStatusWeight =result.getOrderStatusTransactionsWeight();
                    
                    toSend.failures = (result.failures - mLThreadLastIteration.failures)*toSend.nodes;
                    toSend.rdFailures = (result.rdFailures - mLThreadLastIteration.rdFailures)*toSend.nodes;
                    toSend.wrFailures = (result.wrFailures - mLThreadLastIteration.wrFailures)*toSend.nodes;
                    toSend.nrWrFailuresOnCommit = (result.nrWrFailuresOnCommit - mLThreadLastIteration.nrWrFailuresOnCommit)*toSend.nodes;
                    toSend.newOrderFailures = (result.newOrderFailures - mLThreadLastIteration.newOrderFailures)*toSend.nodes;
                    toSend.paymentFailures = (result.paymentFailures - mLThreadLastIteration.paymentFailures)*toSend.nodes;
                    toSend.appFailures = (result.appFailures - mLThreadLastIteration.appFailures)*toSend.nodes;

                    
                    
                                       
                    mLThreadLastIteration = result;
                    
		    return toSend;
	}
   }
   
   
   
    
}
