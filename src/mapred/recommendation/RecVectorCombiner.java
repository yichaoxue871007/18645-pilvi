package mapred.recommendation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author Fangxiaoyu Feng
 * 
 */

public class RecVectorCombiner extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> value,
			Context context)
			throws IOException, InterruptedException {
		
		HashMap<String, Integer> map = new HashMap<String, Integer>();		
		
	    for( Text partialProduct : value ){
			String[] partialProductArray = partialProduct.toString().split(":");
	        for(int i = 0; i < partialProductArray.length; i+=2){
                String itemid = partialProductArray[i];
                int partialV = Integer.parseInt(partialProductArray[i+1]);
    	    	if(!map.containsKey(itemid)){
    	    		map.put(itemid, partialV);
    	    	}else{
        	    	map.put(itemid, map.get(itemid) + partialV);
                }                
        	}
		}

        StringBuilder builder = new StringBuilder();
	    	    
        for(Entry<String, Integer> entry : map.entrySet()){
            builder.append(entry.getKey()).append(":").append(entry.getValue()).append(":");
        }

		context.write(key, new Text(builder.toString()));
	}	
}
