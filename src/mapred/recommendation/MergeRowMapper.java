package mapred.recommendation;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 
 * @author andin
 * 
 */
public class MergeRowMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] split = line.split("\\s+");

		String dirname = ( (FileSplit)context.getInputSplit() ).getPath().getParent().getName();

		if (dirname.startsWith("tr") ) { /* transposed, U */
			context.write(new Text(split[0]), new Text("U#" + split[1]) );
		} else if (dirname.startsWith("co") ) { /* co-occurrence, I */
			context.write(new Text(split[0]), new Text("I#" + split[1]) );
		} else throw new IOException("Folder neither starts with tr or co");
	}
}
