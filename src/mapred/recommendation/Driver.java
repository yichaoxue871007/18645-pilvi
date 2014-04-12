package mapred.recommendation;

import java.io.IOException;
import mapred.job.Optimizedjob;
import mapred.util.SimpleParser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

/**
 * @author Fangxiaoyu Feng (fangxiaf)
 * @author Yichao Xue (yichaox)
 * 
 */
public class Driver {

	public static void main(String args[]) throws Exception {
		SimpleParser parser = new SimpleParser(args);

		String input = parser.get("input");
		String output = parser.get("output");
		String tmpdir = parser.get("tmpdir");

		getItemVector(input, tmpdir + "/item_vector");

		getTrasposedVector(tmpdir + "/item_vector", tmpdir
				+ "/trasposed_vector");

		getCooccurrence(tmpdir + "/trasposed_vector", tmpdir + "/co_occurrence");

		// getCooccurrenceColumn( tmpdir + "/co_occurrence", tmpdir +
		// "/co_occurrence_column");
		//
		// getUserVectorSplitter(tmpdir + "/user_vector", tmpdir+
		// "/splitted_user_vector" );

	}

	private static void getItemVector(String input, String output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
				"Compute User Vector");

		job.setClasses(ItemVectorMapper.class, ItemVectorReducer.class, null);
		job.setMapOutputClasses(Text.class, Text.class);

		job.run();
	}

	private static void getTrasposedVector(String input, String output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
				"Compute Co-occurrence");

		job.setClasses(TransposeMapper.class, TransposeReducer.class, null);
		job.setMapOutputClasses(Text.class, Text.class);

		job.run();
	}

	private static void getCooccurrence(String input, String output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
				"Compute Co-occurrence");

		job.setClasses(CooccurrenceMapper.class, CooccurrenceReducer.class,
				null);
		job.setMapOutputClasses(Text.class, Text.class);

		job.run();
	}

	private static void getCooccurrenceColumn(String input, String output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
				"Compute Co-occurrence");

		job.setClasses(CooccurrenceColumnMapper.class, null, null);
		job.setMapOutputClasses(Text.class, Text.class);

		job.run();
	}

	private static void getUserVectorSplitter(String input, String output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
				"Compute Co-occurrence");

		job.setClasses(UserVectorSplitterMapper.class, null, null);
		job.setMapOutputClasses(Text.class, Text.class);

		job.run();
	}
}
