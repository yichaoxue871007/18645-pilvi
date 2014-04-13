/* author: Fangxiaoyu Feng (fangxiaf) */

package mapred.recommendation;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RecVectorReducer extends Reducer<Text, Text, Text, Text> {
	
	public int topN = 2;

	@Override
	protected void reduce(Text key, Iterable<Text> value,
			Context context)
			throws IOException, InterruptedException {
		
		HashMap<String, Integer> map = new HashMap<String, Integer>();		
		
	    for( Text partialProduct : value ){
			String[] partialProductArray = partialProduct.toString().split(";");

	        for(int i = 0; i < partialProductArray.length; i++){
    	    	String[] temp = partialProductArray[i].split(":");
    	    	if(!map.containsKey(temp[0])){
    	    		map.put(temp[0], 0);
    	    	}
    	    	int partialV = Integer.parseInt(temp[1]);
    	    	map.put(temp[0], map.get(temp[0]) + partialV);
        	}
		}
	    
        Comparator<IDvaluePair> comparator = new Comparator<IDvaluePair>(){
            public int compare(IDvaluePair a, IDvaluePair b)
                {return a.value-b.value;}
        };	    
	    
	    PriorityQueue<IDvaluePair> queue = new PriorityQueue<IDvaluePair>(topN, comparator);
	    
        for(Entry<String, Integer> entry : map.entrySet()){
        	if(queue.size() < topN){
        		queue.offer(new IDvaluePair(entry.getKey(),entry.getValue()));
        	}else{
        		if(entry.getValue() > queue.peek().value){
        			queue.poll();
        			queue.offer(new IDvaluePair(entry.getKey(),entry.getValue()));
        		}
        	}
        }
        
        StringBuilder builder = new StringBuilder();
        
        while (queue.size()!=0) {
        	IDvaluePair pair = queue.poll();
        	builder.insert(0, ";").insert(0, pair.value).insert(0, ":").insert(0, pair.ID);
        }
		context.write(key, new Text(builder.toString()));
	}	
}
