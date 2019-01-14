package com.chen.guo.util;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;

/**
 * Start this before executing ./bin/job-tumbling-window.sh
 */

public class DataServer {
  public static void main(String[] args) throws IOException {
    ServerSocket listener = new ServerSocket(9090);
    try {
      Socket socket = listener.accept();
      System.out.println("Got new connection: " + socket.toString());
      try {
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        Random rand = new Random();
        int count = 0;
        while (true) {
          count++;
          int i = rand.nextInt(10);
          String s = System.currentTimeMillis() + "," + i;
          System.out.println(s);
          out.println(s); //out.print() will screw you up !!!
          if (count % 10 == 0) {
            Thread.sleep(5000); //sleep longer to test the session window
          } else {
            Thread.sleep(500);
          }
        }
      } finally {
        socket.close();
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      listener.close();
    }
  }
}