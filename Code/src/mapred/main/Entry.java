package mapred.main;


public class Entry {
	public static void main(String args[]) throws Exception  {
		
		System.out.println("Starting......");

		long start = System.currentTimeMillis();

		mapred.recommendation.Driver.main(args);
		

		long end = System.currentTimeMillis();

		System.out.println(String.format("Runtime for program recommendation: %d ms",
				end - start));
	}
}
