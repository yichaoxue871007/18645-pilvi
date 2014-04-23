/*
 * 18645 Term Project
 * 
 * Recommendation System Based on Hadoop MapReduce
 * 
 * Fangxiaoyu Feng, Andi Ni, Yichao Xue
 */

package mapred.recommendation;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author andin
 * 
 */
public class MergeRowReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> value, Context context)
			throws IOException, InterruptedException {

		String[] segments = new String[2]; /* 0:U 1:I */

		for (Text item : value) {
			String[] split = item.toString().split("#");
			if (split[0].equals("U") ) {
				segments[0] = split[1];
                        } else if (split[0].equals("I") ) {
				segments[1] = split[1];
			} else throw new IOException("Pair neither starts with U# nor I#");
		}

                if (segments[0] != null && segments[1] != null )
                    context.write(key, new Text(segments[0] + "#" + segments[1]) );
	}
}
