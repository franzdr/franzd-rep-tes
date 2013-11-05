/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.radargun;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.stages.AbstractDistStage;
import org.radargun.stages.TpccBenchmarkStage;
import org.radargun.stages.TransactionRequest;
import org.radargun.state.SlaveState;
import org.radargun.stressors.MLThreadSample;
import org.radargun.stressors.TpccStressorAlternative;
import org.radargun.tpcc.ElementNotFoundException;
import org.radargun.tpcc.TpccTerminal;
import org.radargun.tpcc.transaction.NewOrderTransaction;
import org.radargun.tpcc.transaction.OrderStatusTransaction;
import org.radargun.tpcc.transaction.PaymentTransaction;
import org.radargun.tpcc.transaction.TpccTransaction;
import org.radargun.utils.TypedProperties;
import org.radargun.utils.Utils;

/**
 *
 * @author frank
 */
public class SlaveStandalone extends AbstractDistStage{
   private static Log log = LogFactory.getLog(SlaveStandalone.class);
   public static final int DEFAULT_REQ_PORT = 15000;
    
   CacheWrapper wrapper = null;
   
   private int numOfThreads=1;
   private int nodeIndex = 5;
   //private String configName;
   
   private boolean alreadyInitialized = false;
   
   private Map<Integer , MLThreadSample> mLThreadSamples = new HashMap<Integer, MLThreadSample>();
   private Map<Integer , Lock> threadSamplingLocks = new HashMap<Integer, Lock>();
   private MLThreadSample mLThreadLastIteration = new MLThreadSample();
   private Map<String , Integer> threadNameToId = new HashMap<String, Integer>();
   private Map<Integer, ByteBuffer> threadsBuffers = new HashMap<Integer, ByteBuffer>();
   private AtomicInteger nextFreeThreadId = new AtomicInteger(0);
   private ByteBuffer readBuffer =  ByteBuffer.allocate(DEFAULT_SIZE_READ_BUFFER);;
   private ExecutorService threadPool;
   
   private boolean stopped = false;
   
   private String masterAddress = null;
   private int masterPort = 2103;
   
   private static int DEFAULT_SIZE_READ_BUFFER = 1024;
   
   public SlaveStandalone(){
      Runtime.getRuntime().addShutdownHook(new ShutDownHook("SlaveStandalone process"));
   }
   
   
   public static void main(String[] args) throws Exception{
        SlaveStandalone s = new SlaveStandalone();
        s.setMasterConfig(args);
        s.executeOnSlave();
        ShutDownHook.exit(0);
    }
   
   private  void setMasterConfig(String[] args){
       for (int i = 0; i < args.length - 1; i++) {
         if (args[i].equals("-master")) {
            String param = args[i + 1];
            if (param.contains(":")) {
               masterAddress = param.substring(0, param.indexOf(":"));
               try {
                  masterPort = Integer.parseInt(param.substring(param.indexOf(":") + 1));
               } catch (NumberFormatException nfe) {
                  log.warn("Unable to parse port part of the master!  Failing!");
                  ShutDownHook.exit(10);
               }
            } else {
               masterAddress = param;
            }
         }
      }
      if (masterAddress == null) {
         printUsageAndExit();
      }
   }
    

    @Override
    public DistStageAck executeOnSlave(){
        SocketChannel communicationSocket;
        ServerSocketChannel acceptSocket=null;
        SocketChannel reqSocket;
        Selector socketSelector;
        
            
        
        try {
            socketSelector = Selector.open();
            communicationSocket = SocketChannel.open();
            communicationSocket.configureBlocking(false);
            
            InetSocketAddress isa = new InetSocketAddress(masterAddress , masterPort);
            communicationSocket.connect(isa);
            
            communicationSocket.register(socketSelector, SelectionKey.OP_CONNECT);
            while(!stopped){
                socketSelector.select();
                Set<SelectionKey> readyKeys = socketSelector.selectedKeys();
                Iterator<SelectionKey> it = readyKeys.iterator();
                while(it.hasNext()){
                    SelectionKey key = it.next();
                    it.remove();
                    if(key.isConnectable()){
                        log.info("Connessione al master");
                        //eseguo una prima connessione al master sulla quale scambiare informazioni di gestione
                        //ex.recupero indirizzo dello slave da parte del master
                        if (communicationSocket.isConnectionPending()) {
                            try {
                                communicationSocket.finishConnect();
                            } catch (IOException e) {
                                key.cancel();
                                log.warn("Could not finish connecting. Is the master started?", e);
                                throw e;
                            }
                            key.interestOps(SelectionKey.OP_READ);
                        }
                        log.info("Successfully established connection with master");
                    }else if(key.isAcceptable()){
                        reqSocket = acceptSocket.accept();
                        //log.info("New transaction request received");
                        threadPool.execute(new TransactionRunnable(reqSocket));
                    }
                    else if(key.isReadable()){
                        int readInt = communicationSocket.read(readBuffer);
                        if(readInt==-1){
                            //shutdown dello slave in seguito a shutdown del master
                            log.info("Master shutdown!");
                            key.channel().close();
                            key.cancel();
                            socketSelector.close();
                            acceptSocket.close();
                            stopped=true;
                        }else{
                            log.info("Receiving TpccBenchmarkStage from the Master");
                            int expectedSize = readBuffer.getInt(0);
                            DistStage stage=null;
                            if(readBuffer.position() == 4 + expectedSize){
                                stage = (DistStage) SerializationHelper.deserialize(readBuffer.array(), 4, expectedSize);
                                readBuffer.clear();
                            }
                            if(!alreadyInitialized){
                                setSlaveInfo((TpccBenchmarkStage)stage);
                                buildWrapper();
                                log.info("Costruito wrapper");    
                                //Creo pool di threads di esecuzione transazione
                                setThreadPool();
                                //creo il socket per ricevere le transaction request e lo registro al selector
                                acceptSocket= ServerSocketChannel.open();
                                acceptSocket.configureBlocking(false);
                                acceptSocket.socket().bind(new InetSocketAddress(DEFAULT_REQ_PORT));
                                acceptSocket.register(socketSelector, SelectionKey.OP_ACCEPT);
                                alreadyInitialized = true;
                                log.info("Slave ready to receive transaction requests");
                            }else{//viene ricevuta una optimal config dal master
                                updateConfig((TpccBenchmarkStage)stage);
                            }
                        }
                    }
                }
                
            }
            
        } catch (IOException ex) {
            Logger.getLogger(SlaveStandalone.class.getName()).log(Level.SEVERE, null, ex);
        }
       
       threadPool.shutdown();
       return null;
    }
    
    private void updateConfig(TpccBenchmarkStage stage){
        //updateNumNodes(stage.getActiveSlaveCount());
        updateNumThreads(stage.getNumOfThreads());
    }
    
    /*private void updateNumNodes(int n){

    }*/
    
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
    }
    
    private void setThreadPool(){
        this.threadPool= Executors.newFixedThreadPool(numOfThreads, new ThreadFactory() {
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
    }
    
    
    
    private void setSlaveInfo(TpccBenchmarkStage stage){
        this.nodeIndex = stage.getSlaveIndex();
        this.numOfThreads = stage.getNumOfThreads();
        this.configName = stage.getConfigName();
        this.productName = stage.getProductName();
        
        log.info("TpccBenchmark received, my id is " + this.nodeIndex);
    }
    
    
    private void buildWrapper(){
        this.slaveState = new SlaveState();
        this.initOnSlave(slaveState);
        
        String plugin = Utils.getCacheWrapperFqnClass(this.productName);
        try {
            wrapper = (CacheWrapper) createInstance(plugin);
            wrapper.setUp(this.configName + ".xml" , false, this.nodeIndex, new TypedProperties());
            log.info("SlaveIndex : " + this.nodeIndex);
        } catch (Exception ex) {
            Logger.getLogger(SlaveStandalone.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    
    public class TransactionRunnable implements Runnable{
       MLThreadSample threadSample;
       SocketChannel commSocket;
       ByteBuffer readBuffer;
       int threadId;
       private boolean run = true;
       
       public TransactionRunnable(SocketChannel cs){
           commSocket = cs;
       }

       public void run() {
           threadId = threadNameToId.get(Thread.currentThread().getName());
           readBuffer = threadsBuffers.get(threadId);
           
           try {
            while (run) {
               int readBytes = commSocket.read(readBuffer);
               if(readBytes==-1){
                   commSocket.close();
                   //log.info("Socket closed, thread :" + threadId);
                   run=false;
               }else{
                   if(readBuffer.limit() >= 4){
                        int expectedSize = readBuffer.getInt(0);
                        if(readBuffer.position() == expectedSize + 4){
                            TransactionRequest req = (TransactionRequest) SerializationHelper.deserialize(readBuffer.array(), 4, expectedSize);
                            this.executeTransaction(req);
                            //log.info("Executed transaction, thread :" + threadId);
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
               }

               if (e instanceof Exception) {
                  e.printStackTrace();
               }
            }

             wrapper.endTransaction(successful);
             
        }
       
     }
    
    
    private static void printUsageAndExit() {
      System.out.println("Usage: start_local_slave.sh -master <host>:port");
      System.out.println("       -master: The host(and optional port) on which the master resides. If port is missing it defaults to " + Master.DEFAULT_PORT);
      ShutDownHook.exit(1);
   }
 }

