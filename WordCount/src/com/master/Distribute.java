package com.master;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.StringUtils;

/**
 * @ClassName: Distribute
 * @Description: This class is the implementation of each thread.
 * 			     It will accept its own sub-task and communicate with server
 *               using TCP/IP socket and then store the return result into
 *               its own corresponding instance of class Status.
 * @author Yuan Huang
 * @date 2013-2-11 22:12:22
 */
public class Distribute implements Callable<Status> {

	private List<String> inputWordsList = null;
	private String ip = null;
	private int port = 0;
	private int block = 0;

	public Distribute(List<String> inputWordsList, int block) {
		this.inputWordsList = inputWordsList;
		String client = Processor.getSlave();
		String[] tempstr = client.split(":");
		this.ip = tempstr[0];
		this.port = Integer.parseInt(tempstr[1]);
		this.block = block;
	}
	
	/**
	 * @Title: splitStringToMap
	 * @Description: transfer the single string to a map
	 * @param str
	 * @return Map<String, Integer>
	 */
	private Map<String, Integer> splitStringToMap(String str) {
		Map<String, Integer> map = new HashMap<String, Integer>();
		if (!str.isEmpty()) {
			String[] temp = str.split(",");
			for (int i = 0; i < temp.length; i++) {
				String[] arr = temp[i].split(":");
				map.put(arr[0], Integer.parseInt(arr[1]));
			}
		}
		return map;
	}

	public Status call() throws Exception {
		Status ws = new Status();
		Map<String, Integer> inputWordsMap = new HashMap<String, Integer>();
		// Iterate over the inputWordsList and create a intermediate
		// map with counts
		Socket socket = null;
		BufferedReader br = null;
		PrintWriter pw = null;
		int count = 0;
		while (true) {
			try {
				if (count >= 3) {
					System.out.println("Slave (" + ip + ") has been removed.");
					Processor.delSlave(ip + ":" + port);
					String client = Processor.getSlave();
					String[] tempstr = client.split(":");
					this.ip = tempstr[0];
					this.port = Integer.parseInt(tempstr[1]);
					count = 0;
				}
				socket = new Socket(ip, port);
				br = new BufferedReader(new InputStreamReader(
						socket.getInputStream()));
				pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(
						socket.getOutputStream())));
				String result = StringUtils.join(inputWordsList, " ");
				pw.println(result);
				pw.flush();
				socket.setSoTimeout(500);
				inputWordsMap = splitStringToMap(br.readLine());
				ws.setInputWordsMap(inputWordsMap);
			} catch (IOException e) {
				if (inputWordsMap.isEmpty()) {
					count++;
					System.out.println("Block " + block
							+ " Job Failed! Rollback: " + count + "! "
							+ "Slave Ip: " + ip);
					continue;
				}
			} catch (NullPointerException e) {
				System.err.println("All slaves have been down.");
				System.exit(-1);
			} finally {
				try {
					if (br != null)
						br.close();
					if (pw != null)
						pw.close();
					if (socket != null)
						socket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			break;
		}
		System.out.println("Block " + block + " mapping complete!");
		return ws;
	}

}
