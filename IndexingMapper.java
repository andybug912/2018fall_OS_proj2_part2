import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

public class IndexingMapper extends Mapper<LongWritable,Text,Text,Item> {
	private final static IntWritable one = new IntWritable(1);
	
	@Override
	public void map(LongWritable key, Text value, Context context)
	throws IOException,InterruptedException
	{
		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

		String originLine = value.toString();
		String line[] = originLine.toLowerCase().split(" ");
		for(String wordWithPunctuation: line){
            String[] words = wordWithPunctuation.replaceAll("[!@#$%^&*()-=+,.?<>\'\"]", " ").trim().split(" ");
            for (String word: words) {
                if (word.equals("") || word.charAt(0) < 'a' || word.charAt(0) > 'z') continue;
//                context.write(new Text(word), new Item(new Text(fileName), one));
                context.write(new Text(word), new Item(fileName, 1));
            }
		}
	}
}
