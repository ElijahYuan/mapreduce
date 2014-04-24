package com.master;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName: Reducer
 * @Description: This class is basically to reduce all the intermediate
 *               output to produce final output result
 * @author Yuan Huang
 * @date 2013-2-11 23:39:01
 */
public class Reducer {

	private static Logger log = LoggerFactory.getLogger(Reducer.class
			.getName());

	/**
	 * @Title: reduce
	 * @Description: reduce method
	 * @param wordCountFutureList
	 * @return Map<String,Integer>
	 */
	public Map<String, Integer> reduce(
			List<Future<Status>> wordCountFutureList) {
		Map<String, Integer> wordCountMap = new HashMap<String, Integer>();
		try {
			for (Future<Status> future : wordCountFutureList) {
				Map<String, Integer> tempWordCountMap = future.get()
						.getInputWordsMap();
				if(!tempWordCountMap.isEmpty()) {
					Iterator<Entry<String, Integer>> wordMapIter = tempWordCountMap.entrySet().iterator();
					while (wordMapIter.hasNext()) {
						Entry<String, Integer> inputWord = wordMapIter.next();
						if (wordCountMap.isEmpty()
								|| !wordCountMap.containsKey(inputWord.getKey())) {
							wordCountMap.put(inputWord.getKey(), inputWord.getValue());
						} else {
							int occurence = wordCountMap.get(inputWord.getKey());
							occurence += inputWord.getValue();
							wordCountMap.put(inputWord.getKey(), occurence);
						}
					}
				}
			}
		} catch (InterruptedException e) {
			log.error("Error occured in Reducer" + e.toString());
		} catch (ExecutionException e) {
			log.error("Error occured in Reducer" + e.toString());
		}
		return wordCountMap;
	}

}
