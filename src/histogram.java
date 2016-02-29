// CSC 369 Winter 2016
// Waylin Wang, Myron Zhao Lab7


import java.io.IOException;
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

public class histogram extends Configured implements Tool {

    public static class JsonMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            try {
                JSONObject stat = new JSONObject(value.toString());
                if(stat.has("action")) {
                    JSONObject actionObject = stat.getJSONObject("action");
                    if (actionObject.has("location")) {
                        JSONObject locationObject = actionObject.getJSONObject("location");
                        String outKey = "(" + locationObject.getInt("x") + ", " + locationObject.getInt("y") + ")";
                        context.write(new Text(outKey), new IntWritable(1));
                    }
                }
            }
            catch (Exception e) {
                System.out.println(e);
            }

        }
    }

    public static class JsonReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable temp : values) {
                count++;
            }

            context.write(new Text(key), new IntWritable(count));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = super.getConf();
        Job job = Job.getInstance(conf, "wwang16/mjzhao-lab7-1");

        job.setJarByClass(histogram.class);
        job.setMapperClass(JsonMapper.class);
        job.setReducerClass(JsonReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(MultiLineJsonInputFormat.class);
        MultiLineJsonInputFormat.setInputJsonMember(job, "game");

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