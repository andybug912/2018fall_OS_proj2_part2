import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

public class QueryMapper extends Mapper<LongWritable,Text,Text,Text> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException,InterruptedException
    {
        Configuration conf = context.getConfiguration();
        String keyWordsString = conf.get("KEYWORDS");
        String[] keyWords = keyWordsString.split(" ");
        Set<String> keyWordSet = new HashSet<>(Arrays.asList(keyWords));

        String[] input = value.toString().split("\\{");
        String term = input[0].trim();
        StringBuilder sb = new StringBuilder();
        if (keyWordSet.contains()) {
            String[] items = input[1].substring(0, input[1].length() - 1).split(", ");
            for (String item: items) {
                String fileName = item.split("=")[0];
                sb.append(fileName + " 1");
            }
            context.write(term, sb.toString().trim());
        }
    }
}
