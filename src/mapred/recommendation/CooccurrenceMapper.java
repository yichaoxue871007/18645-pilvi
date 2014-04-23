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
 * @author yichaox
 * @author andin
 * @author Fangxiaoyu Feng
 */
public class CooccurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {

	public final int PrefMatrixItemsRowMaxLength = 2000;

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] split = line.split("\\s+");
		String[] items = split[1].split(":");

		if (items.length > PrefMatrixItemsRowMaxLength)
			return;

		for (int i = 0; i < items.length; i += 2) {
			int count1 = Integer.parseInt(items[i + 1]);

			for (int j = 0; j < items.length; j += 2) {
				int count2 = Integer.parseInt(items[j + 1]);

				context.write(new Text(items[i]), new Text(items[j] + ":"
						+ count1 * count2));
			}
		}
	}
}
