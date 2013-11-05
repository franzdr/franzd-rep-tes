/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.radargun;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.stages.TpccBenchmarkStage;
import org.radargun.stages.TransactionRequest;
import org.radargun.tpcc.TpccTerminal;

/**
 *
 * @author frank
 */
public class PerTransactionProducerThread extends Thread implements MasterProducerThread{
    private static Log log = LogFactory.getLog(PerTransactionProducerThread.class);   
    int transactionType;
       private volatile double transaction_weight;    //an integer in [0,100]
       private double producerRate;
       private Random random;
       private TransactionWeightManager twManager;
       private double arrivalRate;
       private Lock transactionWeightLock;
       private Master master;
       private TransactionRequest req;
       private byte[] reqBytes;
       
       
       public PerTransactionProducerThread(Master m , double arrivalRate, int tt, TransactionWeightManager twManager){
           master = m;
           transactionType = tt;
           this.producerRate = ((arrivalRate / 1000.0) * (this.transaction_weight / 100.0));
           this.random = new Random(System.currentTimeMillis());
           this.twManager = twManager;
           Lock[] weightLocks = twManager.getWeightLocksArray();
           transactionWeightLock = weightLocks[transactionType];
           this.arrivalRate = arrivalRate;
           req = new TransactionRequest(0, transactionType);
       }
       
       
       public void run(){
           //ciclo che invia una richiesta a uno slave random e chiude la connessione
           try {
                master.getStartPoint().await();
                log.info("Starting MasterProducer: " + getName());
           } catch (InterruptedException e) {
                log.warn(e);
           }
           
            
           SocketChannel drawnSlave=null;
           InetSocketAddress isa = null;
           SocketChannel slaveChannel;
           long time;
           
           while(true){
                   drawnSlave = drawRandomSlaveIP();
                   isa = new InetSocketAddress(drawnSlave.socket().getInetAddress(), Master.DEFAULT_REQ_PORT);

               
               try { 
                   slaveChannel = SocketChannel.open();
                   
                   slaveChannel.connect(isa);
                   
                   req.setTimestamp(System.nanoTime());
                   
                   reqBytes = SerializationHelper.prepareForSerialization(req);
                   
                   slaveChannel.write(ByteBuffer.wrap(reqBytes));
                   slaveChannel.close();
                   
                
                   transactionWeightLock.lock();
                
                   if(transactionType == TpccTerminal.NEW_ORDER)
                      transaction_weight = twManager.getNewOrderWeight();
                   else if(transactionType == TpccTerminal.PAYMENT)
                      transaction_weight = twManager.getPaymentWeight();
                   else if(transactionType == TpccTerminal.ORDER_STATUS)
                       transaction_weight = twManager.getOrderStatusWeight();
                   else if(transactionType == TpccTerminal.DELIVERY)
                        transaction_weight = twManager.getDeliveryWeight();
                   else if(transactionType == TpccTerminal.STOCK_LEVEL)
                      transaction_weight = twManager.getStockLevelWeight();
               
                   transactionWeightLock.unlock();
                   
                   
                   
                   this.producerRate = ((arrivalRate / 1000.0) * (this.transaction_weight / 100.0));
                   time = (long) (exp(this.producerRate));
                   
                   Thread.sleep(time);
                   
               
               } catch (IOException ex) {
                   //Logger.getLogger(Master.class.getName()).log(Level.SEVERE, null, ex);
                   log.warn("Slave");
               }catch (InterruptedException ex) {
                   Logger.getLogger(Master.class.getName()).log(Level.SEVERE, null, ex);
               }
               
               
               
               
           }
       }
       
       public SocketChannel drawRandomSlaveIP(){
           int rand = (int) (Math.random()* (master.getSlaves().size()));
           SocketChannel s = master.getSlaves().get(rand);
           return s;
       }
       
       
       private double exp(double rate) {
           return -Math.log(1.0 - random.nextDouble()) / rate;
       }
       
       
   }
