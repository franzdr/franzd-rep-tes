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
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.stages.TransactionRequest;

/**
 *
 * @author frank
 */
public class DrawProducerThread extends Thread implements MasterProducerThread{
       private static Log log = LogFactory.getLog(DrawProducerThread.class);   
       private AtomicLong arrivalRate;
       private Random random;
       private TransactionWeightManager twManager;
       private TransactionRequest req = new TransactionRequest(0, 0);
       private byte[] reqBytes;
       private Master master;
       private int numMasterThreads;
       
       public DrawProducerThread(Master m, int numMasterThreads,int variableArrivalRate, double arrivalRate , TransactionWeightManager twManager){
           master = m;
           this.arrivalRate = new AtomicLong((long)arrivalRate);
           random = new Random(System.currentTimeMillis());
           log.info("ArrivalRate : " + arrivalRate + ", numMasterThreads : " + this.numMasterThreads);
           this.twManager = twManager;
       }
       
       public void run(){
           //ciclo che invia una richiesta a uno slave random e chiude la connessione
           try {
                master.getStartPoint().await();
                log.info("Starting DrawProducerThread : " + getName());
           } catch (InterruptedException e) {
                log.warn(e);
           }
           
           SocketChannel drawnSlave;
           int drawnTransaction;
           InetSocketAddress isa;
           SocketChannel slaveChannel;
           long time;
           double producerRate;
           
           
           while(true){
                   producerRate= arrivalRate.doubleValue() / 1000.0;
                   time =(long) (1/producerRate);
               
                   drawnSlave = drawRandomSlaveIP();
                   drawnTransaction = twManager.choiceTransaction();
                   req.setTransactionType(drawnTransaction);
                   req.setTimestamp(System.nanoTime());
                   isa = new InetSocketAddress(drawnSlave.socket().getInetAddress(), Master.DEFAULT_REQ_PORT);
               
               try { 
                   slaveChannel = SocketChannel.open();
                   slaveChannel.connect(isa);
                   
                   reqBytes = SerializationHelper.prepareForSerialization(req);
                   
                   slaveChannel.write(ByteBuffer.wrap(reqBytes));
                   slaveChannel.close();
                   
                   Thread.sleep(time);
                          
               } catch (IOException ex) {
                   //Logger.getLogger(Master.class.getName()).log(Level.SEVERE, null, ex);
                   log.warn("Lo slave richiesto è in fase di join.");
               }catch (InterruptedException ex) {
                   Logger.getLogger(Master.class.getName()).log(Level.SEVERE, null, ex);
               }   
           }
       }
       
       public void setArrivalRate(double newArrivalRate){
           arrivalRate.getAndSet((long)newArrivalRate);
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
