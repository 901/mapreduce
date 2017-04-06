import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class ChessCount {

   public static int total_games = 0;

   public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
     private final static IntWritable one = new IntWritable(1);
     private Text word = new Text();

     public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
       String line = value.toString();
       switch (line.charAt(11)) {
         case '0':
           word.set("White");
           break;
         case '1':
           word.set("Black");
           break;
         case '2':
           word.set("Draw");
           break;
         default:
           System.err.println("Unknown case");
           break;
       }
       total_games++;
       output.collect(word, one);
     }
   }

   public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text> {
     private Text value = new Text();
     public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
       int sum = 0;
       while (values.hasNext()) {
         sum += values.next().get();
       }
       double ratio = (double)(sum)/total_games;
       value.set(sum + " " + ratio);
       output.collect(key, value);
     }
   }

   public static void main(String[] args) throws Exception {
     JobConf conf = new JobConf(ChessCount.class);
     conf.setJobName("chesscount");

     conf.setOutputKeyClass(Text.class);
     conf.setOutputValueClass(IntWritable.class);

     conf.setMapperClass(Map.class);
     conf.setReducerClass(Reduce.class);

     conf.setInputFormat(TextInputFormat.class);
     conf.setOutputFormat(TextOutputFormat.class);

     FileInputFormat.setInputPaths(conf, new Path(args[0]));
     FileOutputFormat.setOutputPath(conf, new Path(args[1]));

     JobClient.runJob(conf);
   }
}

