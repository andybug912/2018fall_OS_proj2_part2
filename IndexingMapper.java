import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

public class IndexingMapper extends Mapper<LongWritable,Text,Text,Text> {
	@Override
	public void map(LongWritable key, Text value, Context context)
	throws IOException,InterruptedException
	{
		/*Get the name of the file using context.getInputSplit()method*/
		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		String originLine = value.toString();
		//Split the line in words
		String line[] = originLine.toLowerCase().split(" ");
		for(String wordWithPunctuation: line){
            String[] words = wordWithPunctuation.replaceAll("[!@#$%^&*()-=+,.?<>\'\"]", " ").trim().split(" ");
            for (String word: words) {
                if (word.equals("") || word.charAt(0) < 'a' || word.charAt(0) > 'z') continue;
                //for each word emit word as key and file name as value
                context.write(new Text(word), new Text(fileName));
            }
		}
	}
}
