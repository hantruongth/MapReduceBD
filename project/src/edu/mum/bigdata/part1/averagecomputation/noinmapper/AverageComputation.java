package edu.mum.bigdata.part1.averagecomputation.noinmapper;

import edu.mum.bigdata.common.Utility;
import java.io.IOException;

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

public class AverageComputation extends Configured implements Tool
{

    public static class AverageComputationMapper extends Mapper<LongWritable, Text, Text, IntPair>
    {
        private Text word = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String[] lines = value.toString().split("\\s+");
            String ip = lines[0];
            int last = Utility.toNumber(lines[lines.length-1]);
            word.set(ip);
            context.write(word, new IntPair(last, 1));
        }

    }

    public static class AverageComputationReducer extends Reducer<Text, IntPair, Text, DoubleWritable>
    {
        private DoubleWritable result = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<IntPair> values, Context context) throws IOException, InterruptedException
        {
            double sum = 0;
            double count = 0;
            for (IntPair val : values)
            {
                sum += val.getFirstInt();
                count += val.getSecondInt();
            }
            result.set(sum/count);
            context.write(key, result);
        }
    }


    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        //a. automatic removal of 'output' directory before job execution
        org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(conf);
        /*Check if output path (args[1])exist or not*/
        if(fs.exists(new Path(args[1]))){
            /*If exist delete the output path*/
            fs.delete(new Path(args[1]),true);
        }

        int res = ToolRunner.run(conf, new AverageComputation(), args);

        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception
    {

        Job job = new Job(getConf(), "InMapperWordCount");
        job.setJarByClass(AverageComputation.class);

        job.setMapperClass(AverageComputationMapper.class);
        job.setReducerClass(AverageComputationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntPair.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
