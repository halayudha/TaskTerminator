/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.juniarto.taskterminator;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.zeromq.ZMQ;
import java.util.StringTokenizer;
import java.io.File;
import java.lang.ProcessBuilder.Redirect;
/**
 * 
 *
 * @author hduser
 */
public class TaskTerminator {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws InterruptedException {
        // TODO code application logic here
      ZMQ.Context context = ZMQ.context(1);
      
      //Socket to  talk to clients
      ZMQ.Socket responder = context.socket(ZMQ.REP);
      responder.bind("tcp://*:5555");
      
      int i = 0;
      //attempt_1459929106411_0009_m_000000_0

      while (!Thread.currentThread().isInterrupted()){
          
          try {
              byte[] request = responder.recv(0);
              String taskAttemptID = new String(request);
              System.out.println(taskAttemptID);
              //StringTokenizer tokenizer = new StringTokenizer(taskAttemptID,"_");
              String[] tokens = taskAttemptID.split("_");
              if (tokens[0].equals("attempt") && i == 0){
                //Process p = Runtime.getRuntime().exec("/home/hduser/hadoop-2.7.1-src/hadoop-dist/target/hadoop-2.7.1/bin/mapred job -list " );
                //Process p = Runtime.getRuntime().exec("/home/hduser/hadoop-2.7.1-src/hadoop-dist/target/hadoop-2.7.1/bin/mapred job -fail-task " + taskAttemptID);
                ProcessBuilder pb = new ProcessBuilder("mapred","job","-fail-task",taskAttemptID);
                File outputFile = new File("/home/hduser/output.log");
                File errorFile = new File("/home/hduser/error.log");
                pb.redirectOutput(outputFile);
                pb.redirectError(errorFile);
                pb.start();
                i = 1;
              }
              String reply = "World";
              responder.send(reply.getBytes(), 0);
          } catch (IOException ex) {
              Logger.getLogger(TaskTerminator.class.getName()).log(Level.SEVERE, null, ex);
          }
      }
      responder.close();
      context.term();
    }
    
}
