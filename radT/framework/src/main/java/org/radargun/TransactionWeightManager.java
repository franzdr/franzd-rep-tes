/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.radargun;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.tpcc.TpccTerminal;
import org.radargun.tpcc.TpccTools;

/**
 *
 * @author frank
 */
public class TransactionWeightManager extends Thread{
   private static Log log = LogFactory.getLog(TransactionWeightManager.class); 
   private double newOrderWeight; //lock 1
   private double paymentWeight; //lock 2
   private double orderStatusWeight; //lock 3
   private double deliveryWeight; //lock 4
   private double stockLevelWeight; //lock 5
   private Lock[] weightLocks;
   private int totalTransactionTypes = 5;
   private final long updateInterval = 1000L; 
   private int dynamicTransactions;
   private Master master;
          
        public TransactionWeightManager(Master m, int isDynamicTransactionOn, double newOrderWeight , double paymentWeight, double orderStatusWeight){
            this.master = m;
            this.dynamicTransactions = isDynamicTransactionOn;
            if(dynamicTransactions==1){
                double newOrderWeightAbs = 0.2+ Math.pow(Math.sin(0),2); 
                double paymentWeightAbs = 0.1 + 0.8*Math.pow(Math.sin(0),2);
                double orderStatusWeightAbs = 0.1 + Math.pow(Math.sin(0),2);
       
                double norm_den = newOrderWeightAbs + paymentWeightAbs + orderStatusWeightAbs;

                this.newOrderWeight = (newOrderWeightAbs/norm_den)*100;
                this.paymentWeight = (paymentWeightAbs/norm_den)*100;
                this.orderStatusWeight = (orderStatusWeightAbs/norm_den)*100;
            }else{
                this.newOrderWeight = newOrderWeight;
                this.paymentWeight = paymentWeight;
                this.orderStatusWeight = orderStatusWeight;
            }
            weightLocks = new Lock[totalTransactionTypes]; 
              for(int i=0 ; i<totalTransactionTypes ; i++){
                  weightLocks[i] = new ReentrantLock();
              }
        }
          
        public void run(){
           try {
                master.getStartPoint().await();
                log.info("Starting Transactions weight update");
           } catch (InterruptedException e) {
                log.warn(e);
           }
           
           long startTime = System.currentTimeMillis();
           
           while(true){
                try {
                    Thread.sleep(updateInterval);
                } catch (InterruptedException ex) {
                    Logger.getLogger(Master.class.getName()).log(Level.SEVERE, null, ex);
                }
                
                lockAllTransactions();
                updateTransactionWeights((startTime - System.currentTimeMillis())/1000L);
                unlockAllTransactions();
           }
           
        }
        
        public synchronized void updateTransactionWeights(long elapsedSeconds){
            if(this.dynamicTransactions == 1){
                /*double newOrderWeightAbs = 0.1+ 0.9*Math.pow(Math.sin(elapsedSeconds*0.035),2); 
                double paymentWeightAbs = 0.1 + Math.pow(Math.sin(elapsedSeconds*0.0262),2);
                double orderStatusWeightAbs = 0.1 + Math.pow(Math.sin(elapsedSeconds*0.0147),2);*/
                double newOrderWeightAbs = 0.1+ 0.9*Math.pow(Math.sin(elapsedSeconds*0.035),2); 
                double paymentWeightAbs = 0.1 + Math.pow(Math.sin(elapsedSeconds*0.0262+10),2);
                double orderStatusWeightAbs = 0.2 + Math.pow(Math.sin(elapsedSeconds*0.02+30),2);
       
                double norm_den = newOrderWeightAbs + paymentWeightAbs + orderStatusWeightAbs;

        
                this.newOrderWeight = (newOrderWeightAbs/norm_den)*100;
                this.paymentWeight = (paymentWeightAbs/norm_den)*100;
                this.orderStatusWeight = (orderStatusWeightAbs/norm_den)*100;
            }
        }
        
        public synchronized int choiceTransaction() {
            double transactionType = TpccTools.doubleRandomNumber(0, 100);

            if (transactionType <= this.paymentWeight) {
                return TpccTerminal.PAYMENT;
            } else if (transactionType <= this.paymentWeight + this.orderStatusWeight) {
                return TpccTerminal.ORDER_STATUS;
            } else {
                return TpccTerminal.NEW_ORDER;
            }
        }
        
        public Lock[] getWeightLocksArray(){
            return this.weightLocks;
        }
       
        
        public void lockAllTransactions(){
            for(int i=0 ; i<weightLocks.length ; i++)
                weightLocks[i].lock();
        }
        
        public void unlockAllTransactions(){
            for(int i=0 ; i<weightLocks.length ; i++)
                weightLocks[i].unlock();
        }
        
        public double getNewOrderWeight() {
            return newOrderWeight;
        }

        public double getPaymentWeight() {
            return paymentWeight;
        }

        public double getOrderStatusWeight() {
            return orderStatusWeight;
        }
        
        public double getDeliveryWeight(){
            return deliveryWeight;
        }
        
        public double getStockLevelWeight(){
            return stockLevelWeight;
        }
     }
