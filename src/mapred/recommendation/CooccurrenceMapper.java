package mapred.recommendation;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * 
 * @author yichaox
 *
 */
public class CooccurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] items = line.split("\\s+");

		for (int i = 1; i < items.length - 1 && items.length > 2; i++) {
			for (int j = i + 1; j < items.length; j++) {
				if (items[i].compareTo(items[j]) < 0) {
					context.write(new Text(items[i]), new Text(items[j]));
				} else {
					context.write(new Text(items[j]), new Text(items[i]));
				}
			}
		}
	}
}
