
/* author: Fangxiaoyu Feng (fangxiaf) */

package mapred.recommendation;

import java.io.IOException;
import mapred.job.Optimizedjob;
import mapred.util.SimpleParser;

import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.Text;

public class Driver {

	public static void main(String args[]) throws Exception {
		SimpleParser parser = new SimpleParser(args);

		String input = parser.get("input");
		String output = parser.get("output");
		String tmpdir = parser.get("tmpdir");

		getUserverVector(input, tmpdir + "/user_vector");

		getCooccurrence(input, tmpdir + "/co_occurrence");
		
		getCooccurrenceColumn( tmpdir + "/co_occurrence", tmpdir + "/co_occurrence_column");
		
		getUserVectorSplitter(tmpdir + "/user_vector", tmpdir+ "/splitted_user_vector" );

	}

	private static void getUserverVector(String input, String output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
				"Compute User Vector");

		job.setClasses(UserVectorMapper.class, UserVectorReducer.class, null);
		job.setMapOutputClasses(Text.class, Text.class);

		job.run();
	}

	private static void getCooccurrence(String input, String output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
				"Compute Co-occurrence");

		job.setClasses(CooccurrenceMapper.class, CooccurrenceReducer.class, null);
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
