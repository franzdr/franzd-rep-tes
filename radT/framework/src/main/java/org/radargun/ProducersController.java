/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.radargun;

import java.util.ArrayList;
import java.util.List;
import org.radargun.tpcc.TpccTerminal;

/**
 *
 * @author frank
 */
public class ProducersController {
    private Master master;
    private int variableArrivalRate = 0;
    private double maxArrivalRate;
    private int numMasterThreads;
    private List<MasterProducerThread> masterThreads = new ArrayList<MasterProducerThread>();
    
    private double rateStep;
    private int step=1;
    
    public ProducersController(Master m , int numMasterThreads , int variableArrivalRate, double arrivalRate, TransactionWeightManager twManager ){
        this.master = m;
        this.variableArrivalRate = variableArrivalRate;
        this.maxArrivalRate = rateStep = arrivalRate;
        this.numMasterThreads = numMasterThreads;
        if(this.variableArrivalRate != 0){
            this.rateStep = this.maxArrivalRate/this.variableArrivalRate;
        }
        
        if(numMasterThreads == 0){//sistema chiuso   
                masterThreads.add(new PerTransactionProducerThread(master, this.rateStep, TpccTerminal.NEW_ORDER , twManager));
                masterThreads.add(new PerTransactionProducerThread(master, this.rateStep, TpccTerminal.PAYMENT , twManager));
                masterThreads.add(new PerTransactionProducerThread(master, this.rateStep, TpccTerminal.ORDER_STATUS , twManager));
        }else{ //versione con estrazione di transazione da numero arbitrario di threads
                for(int i=0 ; i<numMasterThreads ; i++){
                   MasterProducerThread mProducer = new DrawProducerThread(master, numMasterThreads , variableArrivalRate, this.rateStep/this.numMasterThreads, twManager);
                   masterThreads.add(mProducer);
                }
        }
    }
    
    public void startProducers(){
        for(int j=0 ; j<masterThreads.size() ; j++){
             (masterThreads.get(j)).start();
        }
    }

    
    public void nextArStep(){
        //attivo solo nel caso in cui utilizziamo il sistema aperto
        if(numMasterThreads != 0){
            if(variableArrivalRate != 0 && variableArrivalRate > 0){
                if(step == variableArrivalRate){
                    step = 0;
                }
                step++;
                double newArrivalRateStep = this.rateStep*step;
                for(int j=0 ; j<masterThreads.size() ; j++){
                    ((DrawProducerThread)(masterThreads.get(j))).setArrivalRate(newArrivalRateStep/this.numMasterThreads);
                }
            }
        }
    }
}
