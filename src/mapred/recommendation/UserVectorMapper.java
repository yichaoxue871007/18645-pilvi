/* author: Fangxiaoyu Feng (fangxiaf) */

package mapred.recommendation;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserVectorMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		//String[] words = Tokenizer.tokenize(line);

		String[] split = line.split(": ");
		
		if( split.length > 1 ){
			context.write(new Text(split[0]), new Text(split[1]));
		}

	}
}
