package org.radargun;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.controller.MLController;

/**
 * // TODO: Document this
 *
 * @author mmarkus
 * @since 4.0
 */
public class ShutDownHookMaster extends Thread {

   private static Log log = LogFactory.getLog(ShutDownHook.class);

   private static volatile boolean controlled = false;

   private String processDescription;
   
   private MLController mLController;

   public ShutDownHookMaster(String processDescription, MLController mlController) {
      this.processDescription = processDescription;
      this.mLController = mlController;
   }

   @Override
   public void run() {
      mLController.closeMLControllerSafely();
      if (controlled) {
         log.info(processDescription + " is being shutdown");
      } else {
         log.warn(processDescription + ": unexpected shutdown!");
      }
   }

   public static void exit(int code) {
      controlled = true;
      System.exit(code);
   }
}
