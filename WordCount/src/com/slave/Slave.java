package com.slave;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Slave {

	public static void main(String[] args) {
		if(args.length == 1 && args[0].matches("\\d+")) {
			ServerSocket s = null;
			Socket socket = null;
			try {
				s = new ServerSocket(Integer.parseInt(args[0]));
				while (true) {
					socket = s.accept();
					Thread t = new Thread(new ServeOne(socket));
					t.start();
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				try {
					s.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		} else {
			System.err.println("Usage: java com/slave/Slave <port>");
		}
	}

}