package com.master;

import java.io.File;
import java.util.Map;

public class Master {

	public static void main(String[] args) {
		if(args.length == 2) {
			File input = new File(args[0]);
			if(input.exists()) {
				long startTime = System.currentTimeMillis();
				Processor p = new Processor();
				p.addSlave("127.0.0.1", 6901);
				//p.addSlave("localhost", 6802);
				//p.addSlave("localhost", 6803);
				Map<String, Integer> w = p.processCounting(input, args[1]);
				// p.displayWordCounts(w);
				p.displayByValue(w);
				// p.displayByKey(w);
				long estimatedTime = System.currentTimeMillis() - startTime;
				System.out.print("\nThe total time for running this job is "
						+ estimatedTime + "ms.");
			} else {
				System.err.println("Input file doesn't exist.");
				System.err.println("Usage: java com/master/Master <input> <output>");
			}
		} else {
			System.err.println("Usage: java com/master/Master <input> <output>");
		}
	}

}