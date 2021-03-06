import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class QueryReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Text value, Context context) throws IOException, InterruptedException {

        context.write(key, new Text(value.toString()));
    }
}
