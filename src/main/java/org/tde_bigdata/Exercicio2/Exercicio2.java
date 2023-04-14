package org.tde_bigdata.Exercicio2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Exercicio2 {

    public static Job setupJob(Configuration c) throws IOException {
        Job job = new Job(c, "Ex2");
        Path output = new Path("output/outputEX2");
        job.setJarByClass(Exercicio2.class);
        job.setMapperClass(BackTransactionsMapper.class);
        job.setReducerClass(BackTransactionsReducer.class);
        job.setMapOutputKeyClass(StringDoubleKeys.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(job, output);
        return job;
    }

    public static class BackTransactionsMapper extends Mapper<Object, Text, StringDoubleKeys, IntWritable> {
        private boolean firstLine = true;
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if(firstLine){
                firstLine = false;
                return;
            }
            String[] fields = value.toString().split(";");
            String year = fields[1];
            String flow = fields[4];
            context.write(new StringDoubleKeys(year, flow), new IntWritable(1));
        }
    }

    public static class BackTransactionsReducer extends Reducer<StringDoubleKeys, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(StringDoubleKeys key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for(IntWritable i : values){
                count += i.get();
            }
            context.write(new Text(key.toString()), new IntWritable(count));
        }
    }

}
