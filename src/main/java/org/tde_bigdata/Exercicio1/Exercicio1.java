package org.tde_bigdata.Exercicio1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.tde_bigdata.Exercicio;

import java.io.IOException;
import java.util.Objects;

public class Exercicio1 implements Exercicio {

    @Override
    public Job setupJob(Configuration c) throws IOException {
        Job job = new Job(c, "Ex1");
        Path output = new Path("output/outputEX1");
        job.setJarByClass(Exercicio1.class);
        job.setMapperClass(BackTransactionsMapper.class);
        job.setReducerClass(BackTransactionsReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setCombinerClass(Combiner.class);

        FileOutputFormat.setOutputPath(job, output);
        return job;
    }

    public static class BackTransactionsMapper extends Mapper<Object, Text, Text, IntWritable> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String country = value.toString().split(";")[0];
            if(!Objects.equals(country, "Brazil")) return; // if the country is not brazil, returns, no need to verify directly if its first line or not
            context.write(new Text(country), new IntWritable(1));
        }
    }

    public static class Combiner extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for(IntWritable i : values){ // sums up all the 1's that was written in the map
                count += i.get();
            }
            context.write(key, new IntWritable(count));
        }
    }

    public static class BackTransactionsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0; // same as combiner
            for(IntWritable i : values){
                count += i.get();
            }
            context.write(key, new IntWritable(count));
        }
    }

}
