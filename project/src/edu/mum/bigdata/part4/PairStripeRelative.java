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
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

public class PairStripeRelative extends Configured implements Tool {

    public static class PairRelativeMapper extends Mapper<LongWritable, Text, Pair, IntWritable> {
        private Map<Pair, Integer> map = new TreeMap<>();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().trim().replace(",", "").split("\\s+");
            for (int i = 0; i < words.length; i++) {
                String word = words[i].trim();
                for (int j = i + 1; j < words.length; j++) {
                    if (words[j].equals(word)) {
                        break;
                    } else {
                        Pair k = new Pair(word, words[j]);
                        if (map.containsKey(k)) {
                            map.put(k, map.get(k) + 1);
                        } else {
                            map.put(k, 1);
                        }
                    }
                }
            }
        }

        @Override
        protected void cleanup(Mapper<LongWritable, Text, Pair, IntWritable>.Context context)
                throws IOException, InterruptedException {
            super.cleanup(context);
            for (Map.Entry<Pair, Integer> entry : map.entrySet()) {
                Pair pair = entry.getKey();
                IntWritable count = new IntWritable(entry.getValue());
                context.write(pair, count);
            }
        }

    }

    public static class StripeRelativeReducer extends Reducer<Pair, IntWritable, Text, MyMapWritable> {
        private int sum = 0;
        private MyMapWritable map = new MyMapWritable();
        private String wPrev = null;

        public void reduce(Pair key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            String w = key.getFirst().toString();
            if (!w.equals(wPrev) && wPrev != null) {
                for (Entry<Writable, Writable> entry : map.entrySet()) {
                    FloatWritable value = (FloatWritable) entry.getValue();
                    value.set(value.get() / sum);
                }
                context.write(new Text(wPrev), map);
                sum = 0;
                map.clear();
            }
            for (IntWritable value : values) {
                sum += value.get();
                Text u = new Text(key.getSecond().toString());
                if (map.containsKey(u)) {
                    FloatWritable f = (FloatWritable) map.get(u);
                    f.set(f.get() + value.get());
                } else {
                    map.put(u, new FloatWritable(value.get()));
                }
            }
            wPrev = w;

        }


        @Override
        protected void cleanup(Reducer<Pair, IntWritable, Text, MyMapWritable>.Context context)
                throws IOException, InterruptedException {
            super.cleanup(context);
            for (Entry<Writable, Writable> entry : map.entrySet()) {
                FloatWritable value = (FloatWritable) entry.getValue();
                value.set(value.get() / sum);
            }
            context.write(new Text(wPrev), map);
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(conf);
        if (fs.exists(new Path(args[1]))) {
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
