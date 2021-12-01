import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopFive {
  public static class TopFiveMapper
    extends Mapper<LongWritable, Text, LongWritable, Text>{
      String[] stops = {"he", "she", "they", "the", "a", "an", "are", "you", "of", "is", "and", "or"};
      private final static LongWritable one = new LongWritable(1);
      private org.apache.hadoop.io.Text word = new Text();
      
      
      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        HashSet<String> stopSet = new HashSet();
        for(int i = 0; i < stops.length; i++){
            stopSet.add(stops[i]);
        }
        
        
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            if(!stopSet.contains(word.toString())){
                context.write(one, word);
            }
        }
    
      }
    }

  public static class TopFiveReducer extends Reducer<LongWritable, Text, LongWritable, Text>{
        // private LongWritable result = new LongWritable();
        private Map<String, Integer> wordMap = new HashMap<>();
        private Map<Integer, String> sortedMap = new TreeMap<>(Collections.reverseOrder());

        public void reduce(LongWritable one, Iterable<Text> it, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (Text word : it) {
                count = 1;
                String wordStr = word.toString();
                if(wordMap.containsKey(wordStr)){
                    count = wordMap.get(wordStr);
                    count++;
                }
                wordMap.put(wordStr, count);
            }

            for(Map.Entry<String, Integer> entry : wordMap.entrySet()){
                sortedMap.put(entry.getValue(), entry.getKey());
            }

            int n = 5;
            for(Map.Entry<Integer, String> entry : sortedMap.entrySet()){
                if(n <= 0){
                    break;
                }
                context.write(new LongWritable(entry.getKey()), new Text(entry.getValue()));
                n--;
            }
        }
    }  

  public static void main(String[] args) throws Exception {
    if(args.length != 2){
      System.err.println("Usage: TopFive <input path> <output path>");
      System.exit(-1);
    }

    Job job = new Job();
    job.setJarByClass(TopFive.class);
    job.setJobName("Max temperature");

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(TopFiveMapper.class);
    job.setReducerClass(TopFiveReducer.class);

    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);

    job.setNumReduceTasks(1);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}