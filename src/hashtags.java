// CSC 369 Winter 2016
// Waylin Wang, Myron Zhao Lab7

// run with  hadoop jar job.jar MultilineJsonJob -libjars /path/to/json-20151123.jar,/path/to/json-mapreduce-1.0.jar /input /output


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONObject;

import com.alexholmes.json.mapreduce.MultiLineJsonInputFormat;

public class hashtags extends Configured implements Tool {

    public static class JsonMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text        outputKey   = new Text();
        private IntWritable outputValue = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
        }
    }

    public static class JsonReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = super.getConf();
        Job job = Job.getInstance(conf, "wwang16-lab7-5");

        job.setJarByClass(histogram.class);
        job.setMapperClass(JsonMapper.class);
        job.setReducerClass(JsonReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(MultiLineJsonInputFormat.class);
        MultiLineJsonInputFormat.setInputJsonMember(job, "messageID");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new histogram(), args);
        System.exit(res);
    }
}