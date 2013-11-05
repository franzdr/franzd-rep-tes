/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.radargun.controller;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.Master;
import org.radargun.stressors.MLThreadSample;

/**
 *
 * @author ennio
 */
public class SamplesReceiverThread extends Thread{
    
    private static Log log = LogFactory.getLog(SamplesReceiverThread.class);
    
    private SocketChannel collectorSlaveChannel;
    private Socket slave_channel;
    private InetAddress slave_address;
    private MLThreadSample stat;
    private boolean stop=false;
    private MLController MLController;
    private Master master;
    
    private static final int DEFAULT_STATS_PORT = 5555;

    public SamplesReceiverThread(SocketChannel s, MLController c , Master m){
	this.master = m;
        this.collectorSlaveChannel = s;
        this.MLController = c;
    }
    
    @Override
    public void run(){
       ObjectInputStream  in = null;
 
      try{
          runDiscovery();
          in=new ObjectInputStream(slave_channel.getInputStream());
      }catch(IOException e){                
	   log.warn("Issues instantiating sampler's input stream on controller.");
           stop = true;; 
      }catch(InterruptedException e){                
	   log.warn("Stats collector Discover failed");
	   stop = true;; 
      }
       
      Object obj;
      while(!stop){
         try {
            log.info("Waiting for samples:\n");
            obj=in.readObject();
            if(obj instanceof Integer && (Integer)obj==-1){
                stop=true;
                log.info("Statistics Thread socket has been closed "); 
                break;
            }else{
                master.getProducersController().nextArStep();
                stat=(MLThreadSample)obj;
                MLController.receiveSampleToElaborate(stat);
                log.info(stat.toString());
                log.info("Statistics successfully received.");
                log.info("--------------------------------------");
            }
          }catch (IOException ex) {                      
             log.info("IOEXception"+ex.getMessage());
             stop = true; 
          }catch (ClassNotFoundException e){
             log.info("Error on class recived"+e.getMessage());
             stop = true;; 
         }  
      }

  } 
    
    
    public InetAddress runDiscovery() throws InterruptedException{
       InetAddress addr=null;
       boolean connected=false;
       addr=collectorSlaveChannel.socket().getInetAddress();
       log.info("Connecting to slave 0 in 3000ms in order to collect stats.");
       Thread.sleep(3000);
       while(!connected){
            try {
                slave_channel=new Socket(addr, DEFAULT_STATS_PORT);
                connected = true;
            } catch (IOException ex) {
                Logger.getLogger(SamplesReceiverThread.class.getName()).log(Level.SEVERE, null, ex);
                log.warn("Can't connect to the stats collector slave, i'll sleep for 3000ms.");
                Thread.sleep(3000);
            }
       }
       log.warn("Connection to the stats collector slave established");
       return addr;
    }
    
     public InetAddress getSlave_address() {
        return slave_address;
    }

    public Socket getSlave_channel() {
        return slave_channel;
    }

    public MLThreadSample getStat() {
        return stat;
    }

    public void stopReceiving(){
        this.stop=true;
        try {
            this.slave_channel.close();
        } catch (IOException ex) {
            Logger.getLogger(SamplesReceiverThread.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
