package mapred.recommendation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author yichaox
 * 
 */
public class CooccurrenceReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> value, Context context)
			throws IOException, InterruptedException {

		Map<String, Integer> counts = new HashMap<String, Integer>();

		/* still need to remove duplicate */
		for (Text item : value) {
			String[] split = item.toString().split(":");
			int countNum = Integer.parseInt(split[1]);

			Integer count = counts.get(split[0]);
			if (count == null)
				count = 0;
			count += countNum;
			counts.put(split[0], count);
		}

		StringBuilder sb = new StringBuilder();
		for (Map.Entry<String, Integer> e : counts.entrySet())
			sb.append(e.getKey() + ":" + e.getValue() + ";");

		context.write(key, new Text(sb.toString()));
	}
}
