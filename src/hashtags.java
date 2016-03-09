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

import java.util.HashMap;
import java.util.Set;
import java.util.ArrayList;

import com.alexholmes.json.mapreduce.MultiLineJsonInputFormat;

public class hashtags extends Configured implements Tool {

    public static class JsonMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
           JSONObject json = new JSONObject(value.toString());
           String user = json.getString("user");
           String message = json.getString("text");
           String words[] = message.split(" ");

           for (int i = 0; i < words.length; i++) {
              if (!words[i].equals("a") && !words[i].equals("the") &&
                  !words[i].equals("in") && !words[i].equals("on") &&
                  !words[i].equals("I") && !words[i].equals("he") &&
                  !words[i].equals("she") && !words[i].equals("it") &&
                  !words[i].equals("there") && !words[i].equals("is")) {
                 context.write(new Text(user), new Text(words[i]));
              }
           }

        }
    }

    public static class JsonReducer
            extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
           HashMap<String, Integer> wordCounter = 
            new HashMap<String, Integer>();
           ArrayList<String> list = new ArrayList<String>();
           int maxFreq = 0;
           String word = "";
           String hashKey = "";
           String retVal = "";

           for (Text val : values) {
              word = val.toString();
              if (wordCounter.containsKey(word)) {
                 wordCounter.put(word, wordCounter.get(word) + 1);
              } else {
                 wordCounter.put(word, 1);
              }
           }

           Set<String> keySet = wordCounter.keySet();
           Iterator<String> iter = keySet.iterator();

           while (iter.hasNext()) {
              hashKey = iter.next();
              if (wordCounter.get(hashKey) > maxFreq) {
                 list.clear();
                 list.add(hashKey);
                 maxFreq = wordCounter.get(hashKey);
              } else if (wordCounter.get(hashKey) == maxFreq) {
                 list.add(hashKey);
              }
           }

           for (int i = 0; i < list.size(); i++) {
              retVal += list.get(i);
            
              if (i < list.size() - 1) {
                 retVal += ", ";
              }
           }

           context.write(key, new Text(retVal));

        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = super.getConf();
        Job job = Job.getInstance(conf, "mjzhao-lab7-5");

        job.setJarByClass(hashtags.class);
        job.setMapperClass(JsonMapper.class);
        job.setReducerClass(JsonReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(MultiLineJsonInputFormat.class);
        MultiLineJsonInputFormat.setInputJsonMember(job, "messageID");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new hashtags(), args);
        System.exit(res);
    }
}
