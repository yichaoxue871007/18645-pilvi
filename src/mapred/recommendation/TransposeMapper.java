package mapred.recommendation;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TransposeMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] split = line.split("\\s+");
		String[] uids = split[1].split(";");

		for (String uid : uids) {
			String[] uidCount = uid.split(":");
			context.write(new Text(uidCount[0]), new Text(key.toString() + ":"
					+ uidCount[1]));
		}
	}
}
