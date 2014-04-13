package mapred.recommendation;

import java.io.IOException;
import mapred.job.Optimizedjob;
import mapred.util.SimpleParser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

/**
 * @author Fangxiaoyu Feng (fangxiaf)
 * @author Yichao Xue (yichaox)
 * @author Andi Ni (andin)
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

		mergeMatrixRow( tmpdir + "/transposed_vector",
				tmpdir + "/co_occurrence",
				tmpdir + "/merged" );

		// getCooccurrenceColumn( tmpdir + "/co_occurrence", tmpdir +
		// "/co_occurrence_column");
		//
		// getUserVectorSplitter(tmpdir + "/user_vector", tmpdir+
		// "/splitted_user_vector" );
        
        
     /*   getRecommendation(tmpdir + "/form Andi's output", output);*/

	}
        /* Step 0 */
	private static void getItemVector(String input, String output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
				"Compute User Vector");

		job.setClasses(ItemVectorMapper.class, ItemVectorReducer.class, null);
		job.setMapOutputClasses(Text.class, Text.class);

		job.run();
	}
        /* Step 1 */
	private static void getTrasposedVector(String input, String output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
				"Transpose input matrix");

		job.setClasses(TransposeMapper.class, TransposeReducer.class, null);
		job.setMapOutputClasses(Text.class, Text.class);

		job.run();
	}
        /* Step 2 */
	private static void getCooccurrence(String input, String output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
				"Compute Co-occurrence");

		job.setClasses(CooccurrenceMapper.class, CooccurrenceReducer.class,
				null);
		job.setMapOutputClasses(Text.class, Text.class);

		job.run();
	}
	/* Step 3 */
	private static void mergeMatrixRow(String input, String input_1, String output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
				"Merge rows from Step 1 and Step 2");
		job.addInput(input_1);

		job.setClasses(MergeRowMapper.class, MergeRowReducer.class,
				null);
		job.setMapOutputClasses(Text.class, Text.class);

		job.run();
	}

	private static void getCooccurrenceColumn(String input, String output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
				"Get columns of co-occurrence matrix");

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
    
    private static void getRecommendation(String input, String output)
            throws IOException, ClassNotFoundException, InterruptedException {
		Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
                                            "Compute Recommendation matrix");
        
		job.setClasses(RecVectorMapper.class, RecVectorReducer.class, null);
		job.setMapOutputClasses(Text.class, Text.class);
        
		job.run();
	}
}
