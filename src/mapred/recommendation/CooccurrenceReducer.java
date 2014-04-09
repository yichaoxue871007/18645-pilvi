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
	protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {

		Map<String, Integer> counts = new HashMap<String, Integer>();

		
		for (Text word : value) {
			String w = word.toString();
			Integer count = counts.get(w);
			if (count == null)
				count = 0;
			count++;
			counts.put(w, count);
			
		}

		StringBuilder sb = new StringBuilder();
		for (Map.Entry<String, Integer> e : counts.entrySet())
			sb.append(e.getKey() + ":" + e.getValue() + ";");
		context.write(new Text(key.toString()), new Text(sb.toString()));
	}

}
