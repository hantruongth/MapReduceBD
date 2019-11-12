package edu.mum.bigdata.part5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

public class TemperatureByYear extends Configured implements Tool {

    private static final Logger logger = LogManager.getLogger(TemperatureByYear.class);

    public static class AverageTemMapper extends Mapper<LongWritable, Text, Text, IntPair> {

        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            if(!value.toString().trim().isEmpty()) {

                String[] tmp = value.toString().split(",");
                String year = tmp[0];
                logger.info("******* " + value.toString());

                System.out.println("----------" + value.toString());
                int temp = Integer.parseInt(tmp[1]);
                IntPair p = new IntPair(temp, 1);
                word.set(year);
                context.write(word, p);
            }
        }

    }

    public static class AverageTemReducer extends Reducer<Text, IntPair, Text, FloatWritable> {
        private FloatWritable result = new FloatWritable();

        @Override
        public void reduce(Text key, Iterable<IntPair> values, Context context) throws IOException, InterruptedException {
            float sum = 0;
            float count = 0;
            for (IntPair val : values) {
                sum += val.getFirstInt();
                count++;
            }
            result.set(sum / count);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(conf);
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }

        int res = ToolRunner.run(conf, new TemperatureByYear(), args);

        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        Job job = new Job(getConf(), "TemperatureByYear");
        job.setJarByClass(TemperatureByYear.class);

        job.setMapperClass(AverageTemMapper.class);
        job.setReducerClass(AverageTemReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntPair.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
