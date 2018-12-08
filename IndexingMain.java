import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class IndexingMain {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2) {
            System.err.println("Usage: IndexingMain <input path> <output path>");
            System.exit(-1);
        }

        @SuppressWarnings("deprecation")
		Job job = new Job();
        job.setJarByClass(IndexingMain.class);
        job.setJobName("Indexing");
        job.setMapperClass(IndexingMapper.class);
        job.setReducerClass(IndexingReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
//        job.setInputFormat(TextInputFormat.class);
//        job.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

//        JobConf conf = new JobConf(IndexingMain.class);
//        conf.setJobName("Indexing");
//
//        conf.setOutputKeyClass(Text.class);
//        conf.setOutputValueClass(IntWritable.class);
//
//        conf.setMapperClass(IndexingMapper.class);
////        conf.setCombinerClass(IndexingReducer.class);
//        conf.setReducerClass(IndexingReducer.class);
//        conf.setInputFormat(TextInputFormat.class);
//        conf.setOutputFormat(TextOutputFormat.class);
//        FileInputFormat.setInputPaths(conf, new Path(args[0]));
//        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
//        JobClient.runJob(conf);

    }
}
