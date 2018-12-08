import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IndexingReducer extends Reducer<Text, Text, Text, Text>{
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		HashMap map = new HashMap();
		int count = 0;
		for(Text t: values){
			String str = t.toString();
			if(map != null && map.get(str)!=null){
				count = (int) map.get(str);
				map.put(str, ++count);
			}else{
				map.put(str, 1);
			}
		}
		context.write(key, new Text(map.toString()));
	}
}
