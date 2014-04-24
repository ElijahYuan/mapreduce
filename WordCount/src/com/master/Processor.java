package com.master;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName: Processor
 * @Description: This class is basically used to read the input file and then
 *               configure the thread pools, determine number of threads
 *               available and then send the work to each slave for Mapper.
 *               After obtain the intermediate result from the slave,
 *               it will reduce the results using Reducer and display
 *               the final output of results.
 * @author Yuan Huang
 * @date 2013-2-11 23:34:54
 */
public class Processor {

	private static Logger log = LoggerFactory
			.getLogger(Processor.class.getName());
	private static Queue<String> serverQueue = new LinkedList<String>();
	private int lineNumber = 0;
	private String output = "src/outFile.txt";

	/**
	 * @Title: addSlave
	 * @Description: add a new slave into server Queue
	 * @param ip
	 * @param port
	 * @return void
	 */
	public void addSlave(String ip, int port) {
		serverQueue.offer(ip + ":" + port);
		System.out.println("[Socket = " + ip + ", Port = " + port + "]");
	}

	/**
	 * @Title: getSlave
	 * @Description: get the first slave in the server queue
	 * @return String
	 */
	public static synchronized String getSlave() {
		String client = serverQueue.poll();
		serverQueue.offer(client);
		return client;
	}
	
	/**
	 * @Title: delSlave
	 * @Description: get the specific slave in the server queue
	 * @param slave
	 * @return void
	 */
	public static synchronized void delSlave(String slave) {
		serverQueue.remove(slave);
	}
	
	/**
	 * @Title: processCounting
	 * @Description: This method basically reads the input file and initialized
	 * 				 the thread pool.
	 * @param input
	 * @param output
	 * @return Map<String,Integer>
	 */
	public Map<String, Integer> processCounting(File input, String output) {
		this.output = output;
		BufferedReader br = null;
		FileReader fr = null;
		Map<String, Integer> wordCountMap = null;
		try {
			fr = new FileReader(input);
			FileReader temp = new FileReader(input);
			lineNumber = getLineNumber(temp);
			br = new BufferedReader(fr);
			// Number of threads are determined from run time
			int numberOfThreads = serverQueue.size();
			// System.out.println(numberOfThreads);
			// Thread pool executor initialized
			ThreadPoolExecutor threadpoolExecutor = new ThreadPoolExecutor(
					numberOfThreads, numberOfThreads, 10, TimeUnit.SECONDS,
					new LinkedBlockingQueue<Runnable>());
			// Thread pool executor invoked
			wordCountMap = invokeThreadExecutor(threadpoolExecutor,
					numberOfThreads, br);
		} catch (IOException e) {
			log.error("IOException occured. " + e.toString());
		} finally {
			IOUtils.closeQuietly(br);
			IOUtils.closeQuietly(fr);
		}
		return wordCountMap;
	}

	/**
	 * @Title: invokeThreadExecutor
	 * @Description: Method to actually create and invoke multiple threads to
	 *               process the whole program
	 * @param threadpoolExecutor
	 * @param numberOfThreads
	 * @param br
	 * @throws IOException
	 * @return Map<String,Integer>
	 */
	private Map<String, Integer> invokeThreadExecutor(
			ThreadPoolExecutor threadpoolExecutor, int numberOfThreads,
			BufferedReader br) throws IOException {
		Map<String, Integer> wordCountMap = null;
		List<Future<Status>> wordCountFutureLists = new ArrayList<Future<Status>>();
		int lineCount = 0;
		int block = 0;
		System.out
		.println("::::::::::::::::::::::::::::::::::Calling Mapper to map the words::::::::::::::::::::::::::::::::");
		while (true) {
			List<Callable<Status>> wordCountTasks = new ArrayList<Callable<Status>>();
			int beginOffset = 0;
			int endOffset = 0;
			List<Future<Status>> wordCountFutureList = null;
			try {
				List<String> inputWordsList = getInputList(br, numberOfThreads);
				if (inputWordsList.isEmpty()) {
					if (lineCount != 0) {
						System.out.println("All block(s) mapping complete!");
					}
					break;
				}
				// System.out.println(inputWordsList.size());
				int contentLengthPerThread = getContentLengthPerThread(
						numberOfThreads, inputWordsList);
				endOffset = contentLengthPerThread;
				for (int i = 0; i < numberOfThreads; i++) {
					/*
					 * if content length per thread is equal to the size of
					 * input string list, there would be only one slave to deal
					 * with the whole progress so that when that slave finish
					 * its job, it will show 100% done directly.
					 */
					if (!inputWordsList.isEmpty()) {
						if ((i + 1) == numberOfThreads
								&& endOffset < inputWordsList.size()) {
							endOffset = inputWordsList.size();
						}
						lineCount += (endOffset - beginOffset);
						block++;
						System.out
								.println("Block "
										+ block
										+ " --- "
										+ new DecimalFormat("#.00")
						.format(((float) lineCount / lineNumber) * 100)
						+ "% mapping start");
						List<String> subListInputWords = inputWordsList
								.subList(beginOffset, endOffset);
						Distribute wcm = new Distribute(
								subListInputWords, block);
						wordCountTasks.add(wcm);
						beginOffset = endOffset;
						endOffset = endOffset + contentLengthPerThread;
						if (beginOffset >= inputWordsList.size()
								|| endOffset > inputWordsList.size()) {
							break;
						}
					}
				}
				wordCountFutureList = threadpoolExecutor
						.invokeAll(wordCountTasks);
				wordCountFutureLists.addAll(wordCountFutureList);
			} catch (IndexOutOfBoundsException e) {
				log.error("Exception occured. "
						+ e.toString());
			} catch (InterruptedException e) {
				log.error("Exception occured. "
						+ e.toString());
			}
		}
		Reducer wcr = new Reducer();
		System.out
				.println("::::::::::::::::::::::::::::::::Calling reducer to reduce the words::::::::::::::::::::::::::::::::");
		wordCountMap = wcr.reduce(wordCountFutureLists);
		threadpoolExecutor.shutdown();
		return wordCountMap;
	}

	/**
	 * @Title: getContentLengthPerThread
	 * @Description: calculate the content length for each thread
	 * @param numberOfThreads
	 * @param inputWordsList
	 * @return int
	 */
	private int getContentLengthPerThread(int numberOfThreads,
			List<String> inputWordsList) {
		int contentLengthPerThread = 0;
		if (numberOfThreads == 0) {
			contentLengthPerThread = 1;
		} else {
			contentLengthPerThread = inputWordsList.size() / numberOfThreads;
		}
		if (contentLengthPerThread == 0) {
			contentLengthPerThread = inputWordsList.size();
		}
		return contentLengthPerThread;
	}

	/**
	 * @Title: getLineNumber
	 * @Description: get the line number of input file reader
	 * @param fr
	 * @throws IOException
	 * @return int
	 */
	public int getLineNumber(FileReader fr) {
		LineNumberReader lineNumberReader;
		int lines = 0;
		try {
			lineNumberReader = new LineNumberReader(fr);
			lineNumberReader.skip(Long.MAX_VALUE);
			lines = lineNumberReader.getLineNumber();
		} catch (IOException e) {
			e.printStackTrace();
		}
		// By default, line numbering starts at 0.
		return lines + 1;
	}

	/**
	 * @Title: getInputList
	 * @Description: each thread will be given 800-line strings sub-task
	 * @param br
	 * @param numberOfThreads 
	 * @throws IOException
	 * @return List<String>
	 */
	private List<String> getInputList(BufferedReader br, int numberOfThreads) throws IOException {
		ArrayList<String> inputWordsList = new ArrayList<String>();
		String tempInputWord = null;
		int count = 1;
		while (count <= 800 * numberOfThreads) {
			if ((tempInputWord = br.readLine()) == null)
				break;
			inputWordsList.add(tempInputWord);
			count++;
			// System.out.println(tempInputWord);
		}
		return inputWordsList;
	}

	/**
	 * @Title: displayWordCounts
	 * @Description: display the results
	 * @param wordCountMap
	 * @return void
	 */
	public void displayWordCounts(Map<String, Integer> wordCountMap) {
		ArrayList<Entry<String, Integer>> l = new ArrayList<Entry<String, Integer>>(
				wordCountMap.entrySet());
		displayAndWrite(l);
	}

	/**
	 * @Title: displayByValue
	 * @Description: display the results in occurrence order
	 * @param wordCountMap
	 * @return void
	 */
	public void displayByValue(Map<String, Integer> wordCountMap) {
		ArrayList<Entry<String, Integer>> l = new ArrayList<Entry<String, Integer>>(
				wordCountMap.entrySet());
		Collections.sort(l, new Comparator<Map.Entry<String, Integer>>() {
			public int compare(Map.Entry<String, Integer> o1,
					Map.Entry<String, Integer> o2) {
				return (o2.getValue() - o1.getValue());
			}
		});
		displayAndWrite(l);
	}

	/**
	 * @Title: displayByKey
	 * @Description: display the results in alphabetical order
	 * @param wordCountMap
	 * @return void
	 */
	public void displayByKey(Map<String, Integer> wordCountMap) {
		TreeMap<String, Integer> treemap = new TreeMap<String, Integer>(
				wordCountMap);
		displayWordCounts(treemap);
	}

	/**
	 * @Title: displayAndWrite
	 * @Description: display output result and write it into disk
	 * @param l
	 * @return void
	 */
	public void displayAndWrite(ArrayList<Entry<String, Integer>> l) {
        FileOutputStream output = null;
		try {
			output = new FileOutputStream(new File(this.output));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		System.out.println("WordName" + " - " + "WordCount");
		for (Entry<String, Integer> i : l) {
			String str = i.getKey() + " " + i.getValue() + "\n";
			System.out.print(str);
			try {
				output.write(str.getBytes());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
        try {
			output.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.print("There are " + l.size() + " word(s)");
	}
}
