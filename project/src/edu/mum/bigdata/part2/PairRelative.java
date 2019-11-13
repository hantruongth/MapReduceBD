package edu.mum.bigdata.part2;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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

public class PairRelative extends Configured implements Tool {

    public static class PairRelativeMapper extends Mapper<LongWritable, Text, Pair, DoubleWritable> {
        private final static DoubleWritable one = new DoubleWritable(1);
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

    public static class PairRelativeReducer extends Reducer<Pair, DoubleWritable, Pair, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();
        private double sum = 0;

        @Override
        public void reduce(Pair key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            int s = 0;
            for (DoubleWritable val : values) {
                s += val.get();
            }
            if (key.getSecondString().equals("*"))
                sum = s;
            else {
                result.set(s / sum);
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(conf);
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }
        int res = ToolRunner.run(conf, new PairRelative(), args);

        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        Job job = new Job(getConf(), "PairRelative");
        job.setJarByClass(PairRelative.class);

        job.setMapperClass(PairRelativeMapper.class);
        job.setReducerClass(PairRelativeReducer.class);

        job.setOutputKeyClass(Pair.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
