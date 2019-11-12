package edu.mum.bigdata.part3;

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
import java.util.HashMap;
import java.util.Map;

public class StripeRelative extends Configured implements Tool {

    public static class StripeRelativeMapper extends Mapper<LongWritable, Text, Text, MyMapWritable> {

        private MyMapWritable occurrenceMap = new MyMapWritable();
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\\s+");
            for (int i = 0; i < tokens.length; i++) {
                word.set(tokens[i]);
                occurrenceMap.clear();
                for (int j = i + 1; j < tokens.length; j++) {
                    if (tokens[i] == tokens[j])
                        break;
                    Text neighbor = new Text(tokens[j]);
                    if (!occurrenceMap.containsKey(neighbor)) {
                        occurrenceMap.put(neighbor, new DoubleWritable(1));

                    } else {
                        DoubleWritable count = (DoubleWritable) occurrenceMap.get(neighbor);
                        count.set(count.get() + 1);
                    }
                }

                context.write(word, occurrenceMap);
            }

        }

    }

    public static class StripeRelativeReducer extends Reducer<Text, MyMapWritable, Text, MyMapWritable> {
        private static MyMapWritable combineMapVal = new MyMapWritable();
        private static Map<String, Double> sumMap = new HashMap<String, Double>();

        @Override
        public void reduce(Text key, Iterable<MyMapWritable> values, Context context) throws IOException, InterruptedException {
            combineMapVal.clear();
            for (MapWritable val : values) {
                combineMap(key.toString(), val);
            }
            MyMapWritable map = new MyMapWritable();
            for (Map.Entry<Writable, Writable> item : combineMapVal.entrySet()) {
                DoubleWritable count = (DoubleWritable) combineMapVal.get(item.getKey());
                double d = sumMap.get(key.toString());
                map.put(item.getKey(), new DoubleWritable(count.get() / d));
            }
            context.write(key, map);
        }

        public static void combineMap(String key, MapWritable val) {
            double sum = 0;
            for (Map.Entry<Writable, Writable> item : val.entrySet()) {
                if (!combineMapVal.containsKey(item.getKey())) {
                    combineMapVal.put(item.getKey(), item.getValue());
                } else {
                    DoubleWritable count = (DoubleWritable) combineMapVal.get(item.getKey());
                    count.set(count.get() + ((DoubleWritable) item.getValue()).get());
                }
                sum += ((DoubleWritable) item.getValue()).get();

            }
            if (!sumMap.containsKey(key))
                sumMap.put(key, sum);
            else
                sumMap.put(key, sumMap.get(key) + sum);
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

        int res = ToolRunner.run(conf, new StripeRelative(), args);

        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        Job job = new Job(getConf(), "StripeRelative");
        job.setJarByClass(StripeRelative.class);

        job.setMapperClass(StripeRelativeMapper.class);
        job.setReducerClass(StripeRelativeReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MyMapWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
