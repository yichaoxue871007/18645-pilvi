/*
 * 18645 Term Project
 * 
 * Recommendation System Based on Hadoop MapReduce
 * 
 * Fangxiaoyu Feng, Andi Ni, Yichao Xue
 */

package mapred.recommendation;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author Yichao Xue <yichaox>
 * 
 *         Input format: [Item: UID UID ... UID], Output formant: [Item UID]
 */
public class ItemVectorMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] split = line.split(": ");

		if (split.length > 1) {
			String[] uids = split[1].split("\\s+");

			for (String uid : uids) {
				context.write(new Text(split[0]), new Text(uid));
			}
		}
	}
}
