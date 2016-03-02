// CSC 369 Winter 2016
// Waylin Wang, Myron Zhao Lab7

// run with  hadoop jar job.jar MultilineJsonJob -libjars /path/to/json-20151123.jar,/path/to/json-mapreduce-1.0.jar /input /output


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

public class summaries extends Configured implements Tool {

    public static class JsonMapper
            extends Mapper<LongWritable, Text, IntWritable, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
           try {
              JSONObject json = new JSONObject(value.toString());
              context.write(new IntWritable(json.getInt("game")), 
                            value);
           } catch (Exception e) {
              System.out.println(e);
           }
        }
    }

    public static class JsonReducer
            extends Reducer<IntWritable, Text, Text, Text> {

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, 
         Context context) throws IOException, InterruptedException {

           int finalMove = 1;
           int finalScore = 0;
           int moves = 0;
           int regular = 0;
           int special = 0;
           String outcome = "in progress";
           String actionType = "";
           String userId = "";

           JSONObject json = new JSONObject();
           JSONObject action = new JSONObject();

           for (Text val : values) {
               json = new JSONObject(val.toString());
               action = json.getJSONObject("action");
               actionType = action.getString("actionType");

               moves++;

               if (actionType.equals("Move")) {
                  regular++;
               } else if (actionType.equals("SpecialMove")) {
                  special++;
               } else if (actionType.equals("GameEnd")) {
                  outcome = action.getString("gameStatus");
               }

               if (action.getInt("actionNumber") > finalMove) {
                  finalMove = action.getInt("actionNumber");
                  finalScore = action.getInt("points");
               }
           }

           userId = json.getString("user");

           json = new JSONObject()
                 .put("user", userId)
                 .put("moves", moves)
                 .put("regular", regular)
                 .put("special", special)
                 .put("outcome", outcome)
                 .put("score", finalScore)
                 .put("perMove", (double)finalScore/moves);

           context.write(new Text(""), new Text(json.toString(1)));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = super.getConf();
        Job job = Job.getInstance(conf, "mjzhao-lab7-2");

        job.setJarByClass(summaries.class);
        job.setMapperClass(JsonMapper.class);
        job.setReducerClass(JsonReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(MultiLineJsonInputFormat.class);
        MultiLineJsonInputFormat.setInputJsonMember(job, "game");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new summaries(), args);
        System.exit(res);
    }
}
