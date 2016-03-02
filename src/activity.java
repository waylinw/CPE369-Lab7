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

public class activity extends Configured implements Tool {

    public static class GameMapper
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

    public static class GameReducer
            extends Reducer<IntWritable, Text, IntWritable, Text> {

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, 
         Context context) throws IOException, InterruptedException {

           int finalMove = 1;
           int finalScore = 0;
           int moves = 0;
           int game = 0;
           String outcome = "in progress";
           String userId = "";
           String actionType = "";

           JSONObject json = new JSONObject();
           JSONObject action = new JSONObject();

           for (Text val : values) {
               json = new JSONObject(val.toString());
               action = json.getJSONObject("action");
               actionType = action.getString("actionType");
    
               moves++;

               if (action.getInt("actionNumber") > finalMove) {
                  finalMove = action.getInt("actionNumber");
                  finalScore = action.getInt("points");
               }

               if (actionType.equals("GameEnd")) {
                  outcome = action.getString("gameStatus");
               }
           }

           userId = json.getString("user");
           game = json.getInt("game");

           json = new JSONObject()
                 .put("user", userId)
                 .put("moves", moves)
                 .put("outcome", outcome)
                 .put("score", finalScore);

           context.write(new IntWritable(game), new Text(json.toString(1)));
        }
    }

    public static class UserMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
           try {
              JSONObject json = new JSONObject(value.toString());
              context.write(new Text(json.getString("user")), value);
           } catch (Exception e) {
              System.out.println(e);
           }
        }
    }

    public static class UserReducer
            extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, 
         Context context) throws IOException, InterruptedException {

           JSONObject json = new JSONObject();
           String outcome = "";
           String userId = "";
           int games = 0;
           int won = 0;
           int lost = 0;
           int score = 0;
           int highScore = Integer.MIN_VALUE;
           int moves = 0;
           int longestGame = 0;

           for (Text val : values) {
               json = new JSONObject(val.toString());
               outcome = json.getString("outcome");
               score = json.getInt("score");
               moves = json.getInt("moves");

               games++;

               if (outcome.equals("Loss")) {
                  lost++;
               } else if (outcome.equals("Win")) {
                  won++;
               }

               if (!outcome.equals("in progress")) {
                  if (score > highScore) {
                     highScore = score;
                  }
                  if (moves > longestGame) {
                     longestGame = moves;
                  }
               }
           }

           userId = json.getString("user");

           if (highScore == Integer.MIN_VALUE) {
              highScore = 0;
           }

           json = new JSONObject()
                 .put("games", games)
                 .put("won", won)
                 .put("lost", lost)
                 .put("highscore", highScore)
                 .put("longestGame", longestGame);

           context.write(new Text(userId), new Text(json.toString(1)));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = super.getConf();
        Job job = Job.getInstance(conf, "mjzhao-lab7-3");

        job.setJarByClass(activity.class);
        job.setMapperClass(GameMapper.class);
        job.setReducerClass(GameReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(MultiLineJsonInputFormat.class);
        MultiLineJsonInputFormat.setInputJsonMember(job, "game");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("activity"));

        job.waitForCompletion(true);

        Job userJob = Job.getInstance(conf, "mjzhao-lab7-3-2");
        userJob.setJarByClass(activity.class);
        userJob.setMapperClass(UserMapper.class);
        userJob.setReducerClass(UserReducer.class);
        userJob.setOutputKeyClass(Text.class);
        userJob.setOutputValueClass(Text.class);
        userJob.setInputFormatClass(MultiLineJsonInputFormat.class);
        MultiLineJsonInputFormat.setInputJsonMember(userJob, "user");

        FileInputFormat.addInputPath(userJob, new Path("activity", 
                                     "part-r-00000"));
        FileOutputFormat.setOutputPath(userJob, new Path(args[1]));

        return userJob.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new activity(), args);
        System.exit(res);
    }
}
