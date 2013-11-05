/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.radargun.stages;

import java.io.Serializable;

/**
 *
 * @author frank
 */
public class TransactionRequest implements Serializable{

      private long timestamp;
      private int transactionType;

      public TransactionRequest(long timestamp, int transactionType) {
         this.timestamp = timestamp;
         this.transactionType = transactionType;
      }
      
      public int getTransactionType(){
          return transactionType;
      }
      
      public long getTimeStamp(){
          return this.timestamp;
      }
      
      public void setTimestamp(long ts){
          this.timestamp = ts;
      }
      
      public void setTransactionType(int tt){
          this.transactionType = tt;
      }

   }
