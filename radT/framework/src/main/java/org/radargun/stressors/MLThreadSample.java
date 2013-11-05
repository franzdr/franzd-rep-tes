/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.radargun.stressors;

import java.io.Serializable;
/**
 *
 * @author frank
 */
public class MLThreadSample implements Serializable{
      /*long duration = 0;*/
      int nodes;
      int threads;
      int replicationF;
      int replicationProtocol;
      
      double arrivalRate;

      
      int transactionsAverageIterations = 0;
      double newOrderTransactionsAverage = 0;
      double paymentTransactionsAverage = 0;
      double orderStatusTransactionsAverage = 0;
    
      int reads = 0;
      int writes = 0;
      int newOrderTransactions = 0;
      int paymentTransactions = 0;
      int orderStatusTransactions = 0;
      
      double newOrderPerc = 0;
      double paymentPerc = 0;
      double orderStatusPerc = 0;
      
      double newOrderWeight = 0;
      double paymentWeight = 0;
      double orderStatusWeight = 0;

      int failures = 0;
      int rdFailures = 0;
      int wrFailures = 0;
      int nrWrFailuresOnCommit = 0;
      int newOrderFailures = 0;
      int paymentFailures = 0;
      int newOrderNotAppFailures=0;
      int paymentNotAppFailures=0;
      int orderStatusNotAppFailures=0;
      int orderStatusFailures = 0;
      int appFailures = 0;

      long readsDurations = 0L;
      long writesDurations = 0L;
      long newOrderDurations = 0L;
      long paymentDurations = 0L;
      long orderStatusDurations = 0L;
      long singleNewOrderDurations = 0L;
      long singlePaymentDurations = 0L;
      long singleOrderStatusDurations = 0L;
      long successful_writesDurations = 0L;
      long successful_readsDurations = 0L;
      /*long writeServiceTimes = 0L;
      long readServiceTimes = 0L;
      long newOrderServiceTimes = 0L;
      long paymentServiceTimes = 0L;*/

      long successful_commitWriteDurations = 0L;
      long aborted_commitWriteDurations = 0L;
      long commitWriteDurations = 0L;

      /*long writeInQueueTimes = 0L;
      long readInQueueTimes = 0L;
      long newOrderInQueueTimes = 0L;
      long paymentInQueueTimes = 0L;
      long numWritesDequeued = 0L;
      long numReadsDequeued = 0L;
      long numNewOrderDequeued = 0L;
      long numPaymentDequeued = 0L;*/
    
    /*public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }*/
    public int getNodes(){
        return this.nodes;
    }
    
    public void setNodes(int n){
        this.nodes = n;
    }
    
    public int getThreads(){
        return this.threads;
    }
    
    public void setThreads(int n){
        this.threads = n;
    }
    
    public int getReplicationF(){
        return this.replicationF;
    }
    
    public void setReplicationF(int n){
        this.replicationF = n;
    }
    
    public int getReplicationP(){
        return this.replicationProtocol;
    }
    
    public void setReplicationP(int n){
        this.replicationProtocol = n;
    }
   

    public int getReads() {
        return reads;
    }

    public void setReads(int reads) {
        this.reads = reads;
    }

    public int getWrites() {
        return writes;
    }

    public void setWrites(int writes) {
        this.writes = writes;
    }

    public int getNewOrderTransactions() {
        return newOrderTransactions;
    }

    public void setNewOrderTransactions(int newOrderTransactions) {
        this.newOrderTransactions = newOrderTransactions;
    }

    public int getPaymentTransactions() {
        return paymentTransactions;
    }

    public void setPaymentTransactions(int paymentTransactions) {
        this.paymentTransactions = paymentTransactions;
    }
    
    public int getOrderStatusTransactions() {
        return this.orderStatusTransactions;
    }

    public void setOrderStatusTransactions(int orderStatusTransactions) {
        this.orderStatusTransactions = orderStatusTransactions;
    }
    
    public double getNewOrderTransactionsWeight() {
        return newOrderWeight;
    }

    public void setNewOrderTransactionsWeight(double newOrderTransactions) {
        this.newOrderWeight = newOrderTransactions;
    }

    public double getPaymentTransactionsWeight() {
        return paymentWeight;
    }

    public void setPaymentTransactionsWeight(double paymentTransactions) {
        this.paymentWeight = paymentTransactions;
    }
    
    public double getOrderStatusTransactionsWeight() {
        return orderStatusWeight;
    }

    public void setOrderStatusTransactionsWeight(double orderStatusWeight) {
        this.orderStatusWeight = orderStatusWeight;
    }

    public int getFailures() {
        return failures;
    }

    public void setFailures(int failures) {
        this.failures = failures;
    }

    public int getRdFailures() {
        return rdFailures;
    }

    public void setRdFailures(int rdFailures) {
        this.rdFailures = rdFailures;
    }

    public int getWrFailures() {
        return wrFailures;
    }

    public void setWrFailures(int wrFailures) {
        this.wrFailures = wrFailures;
    }

    public int getNrWrFailuresOnCommit() {
        return nrWrFailuresOnCommit;
    }

    public void setNrWrFailuresOnCommit(int nrWrFailuresOnCommit) {
        this.nrWrFailuresOnCommit = nrWrFailuresOnCommit;
    }

    public int getNewOrderFailures() {
        return newOrderFailures;
    }

    public void setNewOrderFailures(int newOrderFailures) {
        this.newOrderFailures = newOrderFailures;
    }

    public int getPaymentFailures() {
        return paymentFailures;
    }

    public void setPaymentFailures(int paymentFailures) {
        this.paymentFailures = paymentFailures;
    }
    
    public int getOrderStatusFailures() {
        return orderStatusFailures;
    }

    public void setOrderStatusFailures(int orderStatusFailures) {
        this.orderStatusFailures = orderStatusFailures;
    }

    public int getAppFailures() {
        return appFailures;
    }

    public void setAppFailures(int appFailures) {
        this.appFailures = appFailures;
    }

    public long getReadsDurations() {
        return readsDurations;
    }

    public void setReadsDurations(long readsDurations) {
        this.readsDurations = readsDurations;
    }

    public long getWritesDurations() {
        return writesDurations;
    }

    public void setWritesDurations(long writesDurations) {
        this.writesDurations = writesDurations;
    }

    public long getNewOrderDurations() {
        return newOrderDurations;
    }

    public void setNewOrderDurations(long newOrderDurations) {
        this.newOrderDurations = newOrderDurations;
    }

    public long getPaymentDurations() {
        return paymentDurations;
    }

    public void setPaymentDurations(long paymentDurations) {
        this.paymentDurations = paymentDurations;
    }
    
    public long getOrderStatusDurations() {
        return orderStatusDurations;
    }

    public void setOrderStatusDurations(long orderStatusDurations) {
        this.orderStatusDurations = orderStatusDurations;
    }

    public long getSuccessful_writesDurations() {
        return successful_writesDurations;
    }

    public void setSuccessful_writesDurations(long successful_writesDurations) {
        this.successful_writesDurations = successful_writesDurations;
    }

    public long getSuccessful_readsDurations() {
        return successful_readsDurations;
    }

    public void setSuccessful_readsDurations(long successful_readsDurations) {
        this.successful_readsDurations = successful_readsDurations;
    }

    /*public long getWriteServiceTimes() {
        return writeServiceTimes;
    }

    public void setWriteServiceTimes(long writeServiceTimes) {
        this.writeServiceTimes = writeServiceTimes;
    }

    public long getReadServiceTimes() {
        return readServiceTimes;
    }

    public void setReadServiceTimes(long readServiceTimes) {
        this.readServiceTimes = readServiceTimes;
    }

    public long getNewOrderServiceTimes() {
        return newOrderServiceTimes;
    }

    public void setNewOrderServiceTimes(long newOrderServiceTimes) {
        this.newOrderServiceTimes = newOrderServiceTimes;
    }

    public long getPaymentServiceTimes() {
        return paymentServiceTimes;
    }

    public void setPaymentServiceTimes(long paymentServiceTimes) {
        this.paymentServiceTimes = paymentServiceTimes;
    }*/

    public long getSuccessful_commitWriteDurations() {
        return successful_commitWriteDurations;
    }

    public void setSuccessful_commitWriteDurations(long successful_commitWriteDurations) {
        this.successful_commitWriteDurations = successful_commitWriteDurations;
    }

    public long getAborted_commitWriteDurations() {
        return aborted_commitWriteDurations;
    }

    public void setAborted_commitWriteDurations(long aborted_commitWriteDurations) {
        this.aborted_commitWriteDurations = aborted_commitWriteDurations;
    }

    public long getCommitWriteDurations() {
        return commitWriteDurations;
    }

    public void setCommitWriteDurations(long commitWriteDurations) {
        this.commitWriteDurations = commitWriteDurations;
    }

    /*public long getWriteInQueueTimes() {
        return writeInQueueTimes;
    }

    public void setWriteInQueueTimes(long writeInQueueTimes) {
        this.writeInQueueTimes = writeInQueueTimes;
    }

    public long getReadInQueueTimes() {
        return readInQueueTimes;
    }

    public void setReadInQueueTimes(long readInQueueTimes) {
        this.readInQueueTimes = readInQueueTimes;
    }

    public long getNewOrderInQueueTimes() {
        return newOrderInQueueTimes;
    }

    public void setNewOrderInQueueTimes(long newOrderInQueueTimes) {
        this.newOrderInQueueTimes = newOrderInQueueTimes;
    }

    public long getPaymentInQueueTimes() {
        return paymentInQueueTimes;
    }

    public void setPaymentInQueueTimes(long paymentInQueueTimes) {
        this.paymentInQueueTimes = paymentInQueueTimes;
    }

    public long getNumWritesDequeued() {
        return numWritesDequeued;
    }

    public void setNumWritesDequeued(long numWritesDequeued) {
        this.numWritesDequeued = numWritesDequeued;
    }

    public long getNumReadsDequeued() {
        return numReadsDequeued;
    }

    public void setNumReadsDequeued(long numReadsDequeued) {
        this.numReadsDequeued = numReadsDequeued;
    }

    public long getNumNewOrderDequeued() {
        return numNewOrderDequeued;
    }

    public void setNumNewOrderDequeued(long numNewOrderDequeued) {
        this.numNewOrderDequeued = numNewOrderDequeued;
    }

    public long getNumPaymentDequeued() {
        return numPaymentDequeued;
    }

    public void setNumPaymentDequeued(long numPaymentDequeued) {
        this.numPaymentDequeued = numPaymentDequeued;
    }*/

    public String toString(){
      String result="";
      /*result = result + "Duration : " + duration + "\n";*/
      result = result + "AR : " + arrivalRate + "\n";
      result = result + "Reads : " + reads + "\n";
      result = result + "Writes : " + writes + "\n";
      result = result + "NewOrderTransactions : " + newOrderTransactions + "\n";
      result = result + "PaymentTransactions : " + paymentTransactions + "\n";
      result = result + "OrderStatusTransactions : " + orderStatusTransactions + "\n";

      result = result + "Failures : " + failures + "\n";
      result = result + "RdFailures : " + rdFailures + "\n";
      result = result + "WrFailures : " + wrFailures + "\n";
      result = result + "NrWrFailuresOnCommit : " + nrWrFailuresOnCommit + "\n";
      result = result + "NewOrderFailures : " + newOrderFailures + "\n";
      result = result + "PaymentFailures : " + paymentFailures + "\n";
      result = result + "AppFailures : " + appFailures + "\n";

      result = result + "ReadsDurations : " + readsDurations + "\n";
      result = result + "WritesDurations : " + writesDurations + "\n";
      result = result + "NewOrderDurations : " +newOrderDurations + "\n";
      result = result + "PaymentDurations : " + paymentDurations + "\n";
      result = result + "OrderStatusDurations : " + orderStatusDurations + "\n";
      result = result + "Successful_writesDurations : " + successful_writesDurations + "\n";
      result = result + "Successful_readsDurations : " + successful_readsDurations + "\n";
      /*result = result + "WriteServiceTimes : " + writeServiceTimes + "\n";
      result = result + "ReadServiceTimes : " + readServiceTimes + "\n";
      result = result + "NewOrderServiceTimes : " + newOrderServiceTimes + "\n";
      result = result + "PaymentServiceTimes : " + paymentServiceTimes + "\n";*/

      result = result + "Successful_commitWriteDurations : " + successful_commitWriteDurations + "\n";
      result = result + "Aborted_commitWriteDurations : " + aborted_commitWriteDurations + "\n";
      result = result + "CommitWriteDurations : " + commitWriteDurations + "\n";

      /*result = result + "WriteInQueueTimes : " + writeInQueueTimes + "\n";
      result = result + "ReadInQueueTimes : " + readInQueueTimes + "\n";
      result = result + "NewOrderInQueueTimes : " + newOrderInQueueTimes + "\n";
      result = result + "PaymentInQueueTimes : " + paymentInQueueTimes + "\n";
      result = result + "NumWritesDequeued : " + numWritesDequeued + "\n";
      result = result + "NumReadsDequeued : " + numReadsDequeued + "\n";
      result = result + "NumNewOrderDequeued : " + numNewOrderDequeued + "\n";
      result = result + "NumPaymentDequeued : " +numPaymentDequeued + "\n";*/
      
      result = result + "NewOrderAverage : " + newOrderTransactionsAverage + "\n";
      result = result + "PaymentAverage : " + paymentTransactionsAverage + "\n";
      result = result + "OrderStatusAverage : " + orderStatusTransactionsAverage + "\n";
      result = result +"Tot average updates : " + transactionsAverageIterations + "\n";
      
      return result;
    }
    
    public double[] toDoubleArray(){
        double[] result = {
            this.nodes , 
            this.threads , 
            this.replicationF , 
            this.arrivalRate,
            this.newOrderPerc ,
            this.paymentPerc ,
            this.orderStatusPerc ,
            this.singleNewOrderDurations ,
            this.singlePaymentDurations ,
            this.singleOrderStatusDurations
        };
        return result;
    }
    
    public double[] toDoubleArray2(){
        double[] result = {this.nodes , 
            this.threads , 
            this.replicationF , 
            this.replicationProtocol ,
            this.reads ,
            this.writes ,
            this.newOrderTransactions ,
            this.paymentTransactions ,
            this.orderStatusTransactions ,
            this.failures ,
            this.rdFailures ,
            this.wrFailures ,
            this.nrWrFailuresOnCommit ,
            this.newOrderFailures ,
            this.paymentFailures ,
            this.appFailures ,
            this.readsDurations ,
            this.writesDurations ,
            this.newOrderDurations ,
            this.paymentDurations,
            this.successful_writesDurations ,
            this.successful_readsDurations ,
            this.successful_commitWriteDurations ,
            this.aborted_commitWriteDurations ,
            this.commitWriteDurations};
        return result;
    }
    
    
    public void resetTransactionsAverages(){
      transactionsAverageIterations = 0;
      newOrderTransactionsAverage = 0;
      paymentTransactionsAverage = 0;
      orderStatusTransactionsAverage = 0;
    }
    
    public void updateTransactionsAverages(double newOrderWeight ,double paymentWeight ,double orderStatusWeight){
        newOrderTransactionsAverage = (newOrderWeight+(transactionsAverageIterations*newOrderTransactionsAverage))/(transactionsAverageIterations+1);
        paymentTransactionsAverage = (paymentWeight+(transactionsAverageIterations*paymentTransactionsAverage))/(transactionsAverageIterations+1);
        orderStatusTransactionsAverage = (orderStatusWeight+(transactionsAverageIterations*orderStatusTransactionsAverage))/(transactionsAverageIterations+1);


        transactionsAverageIterations++;
    }




    
}
