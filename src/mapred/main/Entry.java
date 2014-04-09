package mapred.main;

import mapred.util.SimpleParser;

public class Entry {
	public static void main(String args[]) throws Exception  {
		SimpleParser parser = new SimpleParser(args);
		//String program = parser.get("program");
		
		//System.out.println("Running program " + program + "..");

		long start = System.currentTimeMillis();

		mapred.recommendation.Driver.main(args);
		

		long end = System.currentTimeMillis();

		System.out.println(String.format("Runtime for program recommendation: %d ms",
				end - start));
	}
}
