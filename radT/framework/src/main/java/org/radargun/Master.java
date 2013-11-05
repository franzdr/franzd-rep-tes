package org.radargun;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.config.MasterConfig;
import org.radargun.state.MasterState;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.radargun.controller.MLController;
import org.radargun.stages.TpccBenchmarkStage;

/**
 * This is the master that will coordinate the {@link Slave}s in order to run the benchmark.
 *
 * @author Mircea.Markus@jboss.com
 */
public class Master {

   private static Log log = LogFactory.getLog(Master.class);
   public static final int DEFAULT_PORT = 2103;
   public static final int DEFAULT_REQ_PORT = 15000;
   public static final int DEFAULT_STANDALONE_REQ_PORT = 15001;
   
   MasterConfig masterConfig;

   private ServerSocketChannel serverSocketChannel;
   private List<SocketChannel> slaves = new ArrayList<SocketChannel>();

   private Map<SocketChannel, ByteBuffer> writeBufferMap = new HashMap<SocketChannel, ByteBuffer>();
   private Map<SocketChannel, ByteBuffer> readBufferMap = new HashMap<SocketChannel, ByteBuffer>();
   private List<DistStageAck> responses = new ArrayList<DistStageAck>();
   private Selector communicationSelector;
   private Selector discoverySelector;
  
   private Map<SocketChannel, Integer> slave2Index = new HashMap<SocketChannel, Integer>();
   private Lock slave2IndexLock = new ReentrantLock();
   
   private MasterState state;
   private MLController MLController;
   
   private int tpccBenchmarkMode = 0;

   private int nextFreeSlaveId = 0;
   private Queue<SocketChannel> communicationChannelsToAddQueue = new LinkedList<SocketChannel>();
   
   private ProducersController producersController;
   
   int processedSlaves = 0;
   private static final int DEFAULT_READ_BUFF_CAPACITY = 1024;

   private CountDownLatch startPoint;
   
   public Master(MasterConfig masterConfig) {
      this.masterConfig = masterConfig;
      state = new MasterState(masterConfig);
      createMLController();
      try {
         communicationSelector = Selector.open();
      } catch (IOException e) {
         throw new IllegalStateException(e);
      }
      Runtime.getRuntime().addShutdownHook(new ShutDownHookMaster("Master process", MLController));
   }
   
   private void createMLController(){
      this.MLController = new MLController(this);
      if(masterConfig.isMLActive()){
          if(masterConfig.isMLTrainingMode()){
              this.MLController.setMLTrainingMode(masterConfig.getMLFileName());
          }else{
              this.MLController.setMLNetworkMode(masterConfig.getMLFileName());
          }
      }
   }

   public void start() throws Exception {
      try {
         startServerSocket();
         runDiscovery();
         prepareNextStage();
         startCommunicationWithSlaves();
      } catch (Exception e) {
         log.error("Exception in start: ", e);
      } finally {
         releaseResources();
         MLController.killReceiverThread();
      }
   }

   private void prepareNextStage() throws Exception {
      DistStage toExecute = state.getNextDistStageToProcess();
      if (toExecute == null) {
         releaseResourcesAndExit();
      } else {
         runDistStage(toExecute, toExecute.getActiveSlaveCount());
      }
   }

   private void runDistStage(DistStage currentStage, int noSlaves) throws Exception {
      writeBufferMap.clear();
      DistStage toSerialize;
      for (int i = 0; i < noSlaves; i++) {
         SocketChannel slave = slaves.get(i);
         slave.configureBlocking(false);
         slave.register(communicationSelector, SelectionKey.OP_WRITE);
         toSerialize = currentStage.clone();
         toSerialize.initOnMaster(state, i);
         if (i == 0) {//only log this once
            if (log.isDebugEnabled())
               log.debug("Starting '" + toSerialize.getClass().getSimpleName() + "' on " + toSerialize.getActiveSlaveCount() + " slave nodes. Details: " + toSerialize);
            else
               log.info("Starting '" + toSerialize.getClass().getSimpleName() + "' on " + toSerialize.getActiveSlaveCount() + " slave nodes.");
         }
         byte[] bytes = SerializationHelper.prepareForSerialization(toSerialize);
         writeBufferMap.put(slave, ByteBuffer.wrap(bytes));
      }
   }

   private void releaseResources() {
      try {
         discoverySelector.close();
      } catch (Throwable e) {
         log.warn(e);
      }
      try {
         communicationSelector.close();
      } catch (Throwable e) {
         log.warn(e);
      }
      for (SocketChannel sc : slaves) {
         try {
            sc.socket().close();
         } catch (Throwable e) {
            log.warn(e);
         }
      }

      try {
         if (serverSocketChannel != null) 
             serverSocketChannel.socket().close();
      } catch (Throwable e) {
         log.warn(e);
      }
   }

   private void runDiscovery() throws IOException {
      discoverySelector = Selector.open();
      serverSocketChannel.register(discoverySelector, SelectionKey.OP_ACCEPT);
      int slaveCount = 0;
      while (slaveCount < masterConfig.getSlaveCount()) {
         log.info("Awaiting registration from " + (masterConfig.getSlaveCount() - slaveCount) + " slaves.");
         discoverySelector.select();
         Set<SelectionKey> keySet = discoverySelector.selectedKeys();
         Iterator<SelectionKey> it = keySet.iterator();
         while (it.hasNext()) {
            SelectionKey selectionKey = it.next();
            it.remove();
            if (!selectionKey.isValid()) {
               continue;
            }
            ServerSocketChannel srvSocketChannel = (ServerSocketChannel) selectionKey.channel();
            SocketChannel socketChannel = srvSocketChannel.accept();            
            
            
            slaves.add(socketChannel);
            slave2Index.put(socketChannel, (nextFreeSlaveId++));
            this.readBufferMap.put(socketChannel, ByteBuffer.allocate(DEFAULT_READ_BUFF_CAPACITY));
            slaveCount++;
            if (log.isTraceEnabled())
               log.trace("Added new slave connection from: " + socketChannel.socket().getInetAddress());
         }
      }
      log.info("Connection established from " + slaveCount + " slaves.");
   }


   private void startCommunicationWithSlaves() throws Exception {
      while (true) {
         communicationSelector.select();
         Set<SelectionKey> keys = communicationSelector.selectedKeys();
         if (keys.size() > 0) {
            Iterator<SelectionKey> keysIt = keys.iterator();
            while (keysIt.hasNext()) {
               SelectionKey key = keysIt.next();
               keysIt.remove();
               if (!key.isValid()) {
                  continue;
               }
               if (key.isWritable()) {
                  if(tpccBenchmarkMode==0)
                    sendStage(key);
                  else
                    sendBStageToNewSlave(key);
               } else if (key.isReadable()) {
                  readStageAck(key);
               } else {
                  log.warn("Unknown selection on key " + key);
               }
            }
         }
         checkChannelsToRegister();
      }
   }
   
   
   private void checkChannelsToRegister(){
       SocketChannel sc;
       while((sc = communicationChannelsToAddQueue.poll()) !=null){
           try {
               sc.register(communicationSelector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);
               log.info("Communication channel registered for a new slave");
           } catch (ClosedChannelException ex) {
               Logger.getLogger(Master.class.getName()).log(Level.SEVERE, null, ex);
           }
       }
   }

   private void readStageAck(SelectionKey key) throws Exception {
      SocketChannel socketChannel = (SocketChannel) key.channel();

      ByteBuffer byteBuffer = readBufferMap.get(socketChannel);
      int value = socketChannel.read(byteBuffer);

      if (value == -1) {
         log.warn("Slave stopped! Index: " + slave2Index.get(socketChannel) + ". Remote socket is: " + socketChannel);
         key.cancel();
         clearSlaveResources(socketChannel);
         if(tpccBenchmarkMode==0)
            releaseResourcesAndExit();
      } else if (byteBuffer.position() >= 4) {
         int expectedSize = byteBuffer.getInt(0);
         if ((expectedSize + 4) > byteBuffer.capacity()) {
            ByteBuffer replacer = ByteBuffer.allocate(expectedSize + 4);
            replacer.put(byteBuffer.array(), 0, byteBuffer.position());
            replacer.put(byteBuffer.array());
            readBufferMap.put(socketChannel, replacer);
            if (log.isTraceEnabled())
               log.trace("Expected size(" + expectedSize + ")" + " is > ByteBuffer's capacity(" +
                     byteBuffer.capacity() + ")" + ".Replacing " + byteBuffer + " with " + replacer);
            byteBuffer = replacer;
         }
         if (log.isTraceEnabled())
            log.trace("Expected size: " + expectedSize + ". byteBuffer.position() == " + byteBuffer.position());
         if (byteBuffer.position() == expectedSize + 4) {
            log.trace("Received ACK from " + socketChannel);
            DistStageAck ack = (DistStageAck) SerializationHelper.deserialize(byteBuffer.array(), 4, expectedSize);
            byteBuffer.clear();
            responses.add(ack);
         }
      }

      if (responses.size() == state.getSlavesCountForCurrentStage()) {
         if (!state.distStageFinished(responses)) {
            log.error("Exiting because issues processing current stage: " + state.getCurrentDistStage());
            releaseResourcesAndExit();
         }
         prepareNextStage();
      }
   }
   
   public void clearSlaveResources(SocketChannel s){
       if (!slaves.remove(s)) {
            throw new IllegalStateException("Socket " + s + " should have been there!");
         }
         if (slave2Index.remove(s)==null) {
            throw new IllegalStateException("SocketIndex of " + s + " should have been there!");
         }
   }
   
   public List<SocketChannel> getSlaves(){
       return this.slaves;
   }
   
   public CountDownLatch getStartPoint(){
       return this.startPoint;
   }
   
   public int getSlavesNumber(){
       return slave2Index.size();
   }
   
   public Lock getSlave2IndexLock(){
       return this.slave2IndexLock;
   }
   
      public SocketChannel getSlaveChannelFromID(int id){
       for(Map.Entry<SocketChannel,Integer> entry : slave2Index.entrySet()){
           if(entry.getValue()==id)
               return entry.getKey();
       }
       return null;
   }
   
   public ProducersController getProducersController(){
        return this.producersController;
   }
   
   public void putInWriteBuffer(SocketChannel sc, ByteBuffer buff){
       writeBufferMap.put(sc,buff);
   }

   
   public Map<SocketChannel,Integer> getSlavesChannel(){
       return slave2Index;
   }
   
   
   public Selector getCommunicationSelector(){
       return this.communicationSelector;
   }

   private void releaseResourcesAndExit() {
      releaseResources();
      ShutDownHook.exit(0);
   }

   private void sendStage(SelectionKey key) throws IOException,Exception {
      SocketChannel socketChannel = (SocketChannel) key.channel();
      ByteBuffer buf = writeBufferMap.get(socketChannel);
      socketChannel.write(buf);
      if (buf.remaining() == 0) {
         log.trace("Finished writing entire buffer");
         key.interestOps(SelectionKey.OP_READ);
         processedSlaves++;
         if (log.isTraceEnabled())
            log.trace("Current stage successfully transmitted to " + processedSlaves + " slave(s).");
      }
      if (processedSlaves == state.getSlavesCountForCurrentStage()) {
         log.trace("Successfully completed broadcasting stage " + state.getCurrentDistStage());
         
         byte[] b = buf.array();
         DistStage toExecute =(DistStage) SerializationHelper.deserialize(b, 4, b.length); 
         if(toExecute instanceof TpccBenchmarkStage){
            StartAlternativeBenchmarkStage(toExecute);
         }
         processedSlaves = 0;
         writeBufferMap.clear();
         responses.clear();
      }
   }
   
   private void StartAlternativeBenchmarkStage(DistStage toExecute) throws InterruptedException{
             List<MasterProducerThread> masterThreads = new ArrayList<MasterProducerThread>();
             
             TpccBenchmarkStage bstage = (TpccBenchmarkStage) toExecute;
             tpccBenchmarkMode = 1;

             startPoint = new CountDownLatch(1);

             //verrà utilizzato per inviare agli slaves le nuove impostazioni per i parametri di controllo
             MLController.setTpccBenchmarkStagePrototype(bstage);
             
             //avvio il thread di sampling
             MLController.setSampleReceiverThread(getSlaveChannelFromID(0));
             MLController.startReceiverThread();
             
             //creo il TransactionWeightManager
             TransactionWeightManager twManager = new TransactionWeightManager(this, bstage.getDynamicTransactions(), bstage.getNewOrderWeight() , bstage.getPaymentWeight() , bstage.getOrderStatusWeight());
             //creo il Producers Controller
             this.producersController = new ProducersController(this, bstage.getNumMasterThreads() , bstage.getVariableArrivalRate(), bstage.getArrivalRate(), twManager);
             //creo il thread di ricezione nuovi slaves
             AcceptingSlavesThread ast = new AcceptingSlavesThread(toExecute);
             
             //avvio l'update delle proporzioni delle transazioni
             twManager.start();
             //avvio i producers
             this.producersController.startProducers();
             //avvio il thread di ricezione nuovi slaves
             ast.start();
             
             startPoint.countDown();

   }

   private void startServerSocket() throws IOException {
      serverSocketChannel = ServerSocketChannel.open();
      serverSocketChannel.configureBlocking(false);
      InetSocketAddress address;
      if (masterConfig.getHost() == null) {
         address = new InetSocketAddress(masterConfig.getPort());
      } else {
         address = new InetSocketAddress(masterConfig.getHost(), masterConfig.getPort());
      }
      serverSocketChannel.socket().bind(address);
      log.info("Master started and listening for connection on: " + address);
      log.info("Waiting 5 seconds for server socket to open completely");
      try 
      {
      	Thread.sleep(5000);
      } catch (InterruptedException ex)
      {
          // ignore
      }
   }
   
   public class AcceptingSlavesThread extends Thread{
         private boolean run = true;
         private DistStage bstage;
         
         public AcceptingSlavesThread(DistStage toExecute){
             bstage = toExecute;
         }

         
         public void run(){
             //serverSocketChannel.register(discoverySelector, SelectionKey.OP_ACCEPT);
             DistStage toSerialize;
             int nextSlaveId=-1;
             
             while (run) {
              try {
                discoverySelector.select();
                log.info("Received Connection from a new slave");
                Set<SelectionKey> keySet = discoverySelector.selectedKeys();
                Iterator<SelectionKey> it = keySet.iterator();
                while (it.hasNext()) {
                    SelectionKey selectionKey = it.next();
                    it.remove();
                    log.info("Controllo se la chiave è valida");
                    if (!selectionKey.isValid()) {
                    continue;
                    }
                    ServerSocketChannel srvSocketChannel = (ServerSocketChannel) selectionKey.channel();
                    SocketChannel socketChannel = srvSocketChannel.accept();
                    socketChannel.configureBlocking(false);
                    
                    //aggiungo le informazioni per la connessione statica
                    nextSlaveId = nextFreeSlaveId++;
                    log.info("Initializing structures for slave : " + nextSlaveId);
                    
                    //il lock serve per evitare che ci sia l'interleaving fra l'invio del benchmarkStage ad uno
                    //slave appena aggiunto e l'invio a tutti gli slave di un aggiornamento dei parametri di controllo
                    slave2IndexLock.lock();
                    try{
                        slave2Index.put(socketChannel, nextSlaveId);
                        readBufferMap.put(socketChannel, ByteBuffer.allocate(DEFAULT_READ_BUFF_CAPACITY));
                        //aggiungo le informazioni per l'esecuzione del benchmark alternativo
                    
                        //preparo il buffer di scrittura per l'invio del BenchmarkStage al nuovo slave
                        this.bstage = MLController.getUpdateTpccBenchmarkPrototype();
                        toSerialize = bstage.clone();
                        toSerialize.initOnMaster(state, nextSlaveId);
                        ((TpccBenchmarkStage)toSerialize).setConfigName(state.configNameOfTheCurrentBenchmark());
                    }finally{
                     slave2IndexLock.unlock();
                    }
                    byte[] bytes = SerializationHelper.prepareForSerialization(toSerialize);
                    writeBufferMap.put(socketChannel, ByteBuffer.wrap(bytes));
                    communicationChannelsToAddQueue.add(socketChannel);
                    communicationSelector.wakeup();
                    if (log.isTraceEnabled())
                      log.trace("Added new slave connection from: " + socketChannel.socket().getInetAddress());
                 }
                }catch (IOException ex) {
                     Logger.getLogger(Master.class.getName()).log(Level.SEVERE, null, ex);
                }
                log.info("Connection established from slave " + nextSlaveId);   
             }
         }
    }
     
    public void clearWriteBuffer(){
        this.writeBufferMap.clear();
    }
     
     private void sendBStageToNewSlave(SelectionKey key){
        SocketChannel sc = (SocketChannel) key.channel();
        ByteBuffer buf = writeBufferMap.get(sc);
        try {
            sc.write(buf);
        } catch (IOException ex) {
            Logger.getLogger(Master.class.getName()).log(Level.SEVERE, null, ex);
        }
        log.info(buf.remaining());
        if (buf.remaining() == 0) {
         log.info("BenchmarkStage transmitted to slave " + slave2Index.get(sc));
         key.interestOps(SelectionKey.OP_READ);
         if (log.isTraceEnabled())
            log.trace("BenchmarkStage transmitted to slave " + slave2Index.get(sc));
      }
        //ora che lo slave ha ricevuto il benchmark stage permetto al master di richiedergli transazioni
        //il master sceglie gli slaves a cui richiedere transazioni da "slaves"
        if(!slaves.contains(sc))
            slaves.add(sc);
     }
}
