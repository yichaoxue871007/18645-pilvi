package mapred.recommendation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author violethaze
 * 
 */
public class ItemVectorReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> value, Context context)
			throws IOException, InterruptedException {

		Map<String, Integer> counts = new HashMap<String, Integer>();

		/* get histogram of UIDs */
		for (Text uid : value) {
			String u = uid.toString();
			Integer count = counts.get(u);
			if (count == null)
				count = 0;
			count++;
			counts.put(u, count);
		}

		StringBuilder sb = new StringBuilder();
		for (Map.Entry<String, Integer> e : counts.entrySet())
			sb.append(e.getKey() + ":" + e.getValue() + ";");
		context.write(key, new Text(sb.toString()));
	}
}
