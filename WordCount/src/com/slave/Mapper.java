package com.slave;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

/**
 * @ClassName: Mapper
 * @Description: This class is used to map the input words by creating a map of
 *               all input words along with their count of concurrence.
 * @author Yuan Huang
 * @date 2013-2-11 23:40:43
 */
public class Mapper {
	private static final String PATTERN_SPACE = " ";
	private static final Integer FIRST_OCCURENCE = 1;
	private static final int LENGTH_ONE = 1;
	private String inputWordsList = null;

	public Mapper(String inputWordsList) {
		this.inputWordsList = inputWordsList;
	}

	private String mapToString(Map<String, Integer> map) {
		StringBuilder str = new StringBuilder();
		Iterator<Entry<String, Integer>> i = map.entrySet().iterator();
		while(true) {
			Entry<String, Integer> k = i.next();
			str.append(k.getKey());
			str.append(":");
			str.append(k.getValue().toString());
			if(!i.hasNext())
				return str.toString();
			str.append(",");
		}
	}
	
	public String mapAndCombine() {
		String result = "";
		Map<String, Integer> inputWordsMap = new HashMap<String, Integer>();
		// Iterate over the inputWordsList and create a intermediate
		// map with counts
		StringTokenizer inputWordsTokens = new StringTokenizer(inputWordsList.trim(),
				PATTERN_SPACE);
		while (inputWordsTokens.hasMoreTokens()) {
			String inputWordMapKey = inputWordsTokens.nextToken().replaceAll(
					"[^a-z]", "");
			// Filter and remove commas and empty spaces
			if (inputWordMapKey != null
					&& inputWordMapKey.length() >= LENGTH_ONE) {
				inputWordMapKey = inputWordMapKey.toLowerCase();
				if (!inputWordsMap.isEmpty()
						&& inputWordsMap.containsKey(inputWordMapKey)) {
					int wordCount = inputWordsMap.get(inputWordMapKey);
					wordCount++;
					inputWordsMap.put(inputWordMapKey, wordCount);
				} else {
					inputWordsMap.put(inputWordMapKey, FIRST_OCCURENCE);
				}
			}

		}
		// Validate the map to see if its empty or not
		if (!inputWordsMap.isEmpty()) {
			result = mapToString(inputWordsMap);
		}

		return result;
	}

}
