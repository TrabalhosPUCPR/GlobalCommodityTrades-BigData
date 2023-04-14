package org.example.Exercicio1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Objects;

public class Exercicio1 {

    public static Job setupJob(Configuration c) throws IOException {
        Job job = new Job(c, "Ex1");
        Path output = new Path("output/outputEX1");
        job.setJarByClass(Exercicio1.class);
        job.setMapperClass(BackTransactionsMapper.class);
        job.setReducerClass(BackTransactionsReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(job, output);
        return job;
    }

    public static class BackTransactionsMapper extends Mapper<Object, Text, Text, IntWritable> {
        private boolean firstLine = true;
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if(firstLine){
                firstLine = false;
                return;
            }
            String country = value.toString().split(";")[0];
            if(!Objects.equals(country, "Brazil")) return;
            context.write(new Text(country), new IntWritable(1));
        }
    }

    public static class BackTransactionsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for(IntWritable i : values){
                count += i.get();
            }
            context.write(key, new IntWritable(count));
        }
    }

}
