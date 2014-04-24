package com.master;

import java.util.Map;

/**
 * @ClassName: Status
 * @Description: Class to store the status of all the intermediate
 *               results and act as an input for reducer for final
 *               word count computation
 * @author Yuan Huang
 * @date 2013-2-11 23:38:16
 */
public class Status {

	private Map<String, Integer> inputWordsMap = null;

	public Map<String, Integer> getInputWordsMap() {
		return inputWordsMap;
	}

	public void setInputWordsMap(Map<String, Integer> inputWordsMap) {
		this.inputWordsMap = inputWordsMap;
	}

}
