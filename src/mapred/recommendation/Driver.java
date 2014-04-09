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

	}

	private static void getUserverVector(String input, String output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
				"Compute User Vector");

		job.setClasses(UserVectorMapper.class, UserVectorReducer.class, null);
		job.setMapOutputClasses(Text.class, Text.class);

		job.run();
	}	
}
