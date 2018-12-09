import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IndexingReducer extends Reducer<Text, Item, Text, Text>{
	@Override
	public void reduce(Text key, Iterable<Item> values, Context context) throws IOException, InterruptedException {
		HashMap<String, Integer> map = new HashMap<>();
		for(Item item: values){
			String fileName = item.getFileName();
			int count = item.getCount();
			map.put(fileName, map.getOrDefault(fileName, 0) + count);
		}
		context.write(key, new Text(map.toString()));
	}
}
