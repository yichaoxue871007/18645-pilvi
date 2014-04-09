/* author: Fangxiaoyu Feng (fangxiaf) */

package mapred.recommendation;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserVectorReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> value,
			Context context)
			throws IOException, InterruptedException {

	    for( Text valueitem : value ){
			String[] vector = valueitem.toString().split(" ");
        	StringBuilder item = new StringBuilder();

	        for(int i = 0; i < vector.length; i++){
    	    	item.append(vector[i]).append(":1.0;");
        	}
			context.write(key, new Text(item.toString()));
		}
	}
}
