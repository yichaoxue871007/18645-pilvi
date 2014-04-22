package mapred.recommendation;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TransposeReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> value, Context context)
			throws IOException, InterruptedException {

		StringBuilder sb = new StringBuilder();
		for (Text item : value) {
			sb.append(item.toString() + ":");
		}

		context.write(key, new Text(sb.toString()));
	}

}
