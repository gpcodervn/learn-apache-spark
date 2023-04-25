package com.gpcoder.serverlog;

import org.apache.commons.lang3.RandomUtils;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

public class LoggingServer {

    public static final int PORT = 8989;
    public static final String LOG_SEPARATOR = ",";
    private static final String[] SEVERITIES = {"DEBUG", "INFO", "WARN", "ERROR"};

    public static void main(String[] args) throws IOException, InterruptedException {
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            while (true) { // Allow multiple connections
                Socket socket = serverSocket.accept();
                System.out.println("Accepted a connection");

                while (true) { // Send log forever
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    String level = SEVERITIES[RandomUtils.nextInt(0, SEVERITIES.length)];
                    out.println(level + LOG_SEPARATOR + new java.util.Date());
                    TimeUnit.MILLISECONDS.sleep(1);
                }
            }
        }
    }
}
