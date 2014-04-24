package com.slave;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;

public class ServeOne implements Runnable {

	private Socket socket = null;
	private BufferedReader br = null;
	private PrintWriter pw = null;

	public ServeOne(Socket s) {
		socket = s;
		try {
			br = new BufferedReader(new InputStreamReader(
					socket.getInputStream()));
			pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(
					socket.getOutputStream())), true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void run() {
		String str;
		try {
			str = br.readLine();
			pw.println(new Mapper(str).mapAndCombine());
			pw.flush();
		} catch (Exception e) {
			try {
				br.close();
				pw.close();
				socket.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
	}
}
