package mapred.recommendation;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author yichaox
 * @author andin
 * 
 */
public class CooccurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {

        public final int PrefMatrixItemsRowMaxLength = 2000;

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] split = line.split("\\s+");
		String[] items = split[1].split(";");

                if (items.length > PrefMatrixItemsRowMaxLength) return;

		for (int i = 0; i < items.length; i++) {
			String[] i1 = items[i].split(":");
			int count1 = Integer.parseInt(i1[1]);

			for (int j = 0; j < items.length; j++) {
				String[] i2 = items[j].split(":");
				int count2 = Integer.parseInt(i2[1]);

				context.write(new Text(i1[0]), new Text(i2[0] + ":" + count1
						* count2));
			}
		}
	}
}
