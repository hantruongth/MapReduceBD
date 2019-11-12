package edu.mum.bigdata.part4;

import edu.mum.bigdata.common.MyMapWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

public class PairStripeRelative extends Configured implements Tool {

    public static class PairRelativeMapper extends Mapper<LongWritable, Text, Pair, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        //private Text word = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\\s+");
            for (int i = 0; i < tokens.length; i++) {
                for (int j = i + 1; j < tokens.length; j++) {
                    if (tokens[i] == tokens[j])
                        break;
                    Pair p = new Pair(tokens[i], tokens[j]);
                    Pair p2 = new Pair(tokens[i], "*");
                    context.write(p, one);
                    context.write(p2, one);
                }
            }

        }

    }

    public static class StripeRelativeReducer extends Reducer<Pair, IntWritable, Text, MyMapWritable> {
        private Text result = new Text();
        //private static MyMapWritable combineMapVal = new MyMapWritable();
        private static Map<String, MyMapWritable> combineMapVal = new HashMap<String, MyMapWritable>();
        private static Map<String, Double> sumMap = new HashMap<String, Double>();
        private static List<String> keyList = new ArrayList<String>();

        @Override
        public void reduce(Pair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            if (key.getSecondString().equals("*"))
                sumMap.put(key.getFirstString(), sum);
            else
                combineMap(key, sum);
        }

        public static void combineMap(Pair key, Double sum) {
            if (!combineMapVal.containsKey(key.getFirstString())) {
                keyList.add(key.getFirstString());
                MyMapWritable mapVal = new MyMapWritable();
                mapVal.put(new Text(key.getSecondString()), new DoubleWritable(sum));
                combineMapVal.put(key.getFirstString(), mapVal);
            } else {
                MyMapWritable occurrenceMap = combineMapVal.get(key.getFirstString());
                Text neighbor = new Text(key.getSecondString());
                if (!occurrenceMap.containsKey(neighbor)) {
                    occurrenceMap.put(neighbor, new DoubleWritable(sum));

                } else {
                    DoubleWritable count = (DoubleWritable) occurrenceMap.get(neighbor);
                    count.set(count.get() + sum);
                }
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            Collections.sort(keyList);
            for (String item : keyList) {
                MyMapWritable myMap = new MyMapWritable();
                for (Entry<Writable, Writable> neighbor : combineMapVal.get(item).entrySet()) {
                    DoubleWritable val = (DoubleWritable) neighbor.getValue();
                    myMap.put(neighbor.getKey(), new DoubleWritable(val.get() / sumMap.get(item)));
                }
                result.set(item);

                context.write(result, myMap);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //a. automatic removal of 'output' directory before job execution
        org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(conf);
        /*Check if output path (args[1])exist or not*/
        if (fs.exists(new Path(args[1]))) {
            /*If exist delete the output path*/
            fs.delete(new Path(args[1]), true);
        }

        int res = ToolRunner.run(conf, new PairStripeRelative(), args);

        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        Job job = new Job(getConf(), "PairStripeRelative");
        job.setJarByClass(PairStripeRelative.class);

        job.setMapperClass(PairRelativeMapper.class);
        job.setReducerClass(StripeRelativeReducer.class);

        job.setMapOutputKeyClass(Pair.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MyMapWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
