import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IndexingCombiner extends Reducer<Text, Item, Text, Item>{
	@Override
	public void reduce(Text key, Iterable<Item> values, Context context) throws IOException, InterruptedException {
		HashMap<String, Integer> map = new HashMap<>();
		for(Item item: values){
			String fileName = item.getFileName().toString();
			int count = item.getCount().get();
			map.put(fileName, map.getOrDefault(fileName, 0) + count);
		}
		Iterator<Entry<String, Integer>> iterator = map.entrySet().iterator();
		while (iterator.hasNext()) {
			@SuppressWarnings("unchecked")
			Map.Entry<String, Integer> pair = (Map.Entry<String, Integer>) iterator.next();
			context.write(key, new Item(new Text(pair.getKey()), new IntWritable(pair.getValue())));
		}
	}
}
