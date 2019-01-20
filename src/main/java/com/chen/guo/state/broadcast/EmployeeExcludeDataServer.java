package com.chen.guo.state.broadcast;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class EmployeeExcludeDataServer {
  public static void main(String[] args) throws Exception {
    final String employeeToExclude = EmployeeExcludeDataServer.class.getResource("/state/broadcast/currentEmployees.txt").toURI().getPath();

    ServerSocket listener = new ServerSocket(9090);
    BufferedReader br = null;
    try {
      Socket socket = listener.accept();
      System.out.println("Got new connection: " + socket.toString());

      br = new BufferedReader(new FileReader(employeeToExclude));
      try {
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        String line;

        while ((line = br.readLine()) != null) {
          out.println(line);
          //Thread.sleep(100);
        }
      } finally {
        socket.close();
      }

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      listener.close();
      if (br != null)
        br.close();
    }
  }
}

