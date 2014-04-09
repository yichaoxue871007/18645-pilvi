package mapred.recommendation;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserVectorSplitterMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] uidAndItems = line.split("\\s+", 2);
		String[] items = uidAndItems[1].split(";");
		
		for(String item: items){
			String[] w = item.split(":");
			
			context.write(new Text(w[0]), new Text(uidAndItems[0]+":"+w[1]));
		}
	}

}
