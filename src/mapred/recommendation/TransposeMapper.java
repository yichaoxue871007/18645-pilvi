package mapred.recommendation;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Yichao Xue
 * @author Fangxiaoyu Feng
 * 
 */

public class TransposeMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] split = line.split("\\s+");
		String[] uids = split[1].split(":");

		/*for (String uid : uids) {
			String[] uidCount = uid.split(":");
			context.write(new Text(uidCount[0]), new Text(split[0] + ":"
					+ uidCount[1]));
		}*/

		for (int i = 0; i < uids.length; i+=2) {
			//String[] uidCount = uid.split(":");
			context.write(new Text(uids[i]), new Text(split[0] + ":"
					+ uids[i+1]));
		}

	}
}
