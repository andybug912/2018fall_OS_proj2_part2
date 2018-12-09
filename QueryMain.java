import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class QueryMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length <= 2) {
            System.err.println("Usage: IndexingMain <input path> <output path> <key word1> <key word2> ...");
            System.exit(-1);
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 2; i < args.length; i++) {
            sb.append(args[i] + " ");
        }
        String keyWords = sb.toString().trim();

        Configuration conf = new Configuration();
        conf.set("KEYWORDS", keyWords);
        Job job = Job.getInstance(conf, "Indexing");
        job.setJarByClass(QueryMain.class);
        job.setMapperClass(QueryMapper.class);
//        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(QueryReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
