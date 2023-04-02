package org.example;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Objects;


public class Exercicio4 {

    public static Job setupJob(Configuration c) throws IOException {
        Job job = new Job(c, "Ex4");
        Path output = new Path("output/outputEX4");
        job.setJarByClass(Exercicio4.class);
        job.setMapperClass(BackTransactionsMapper.class);
        job.setReducerClass(BackTransactionsReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileOutputFormat.setOutputPath(job, output);
        return job;
    }

    public static class BackTransactionsMapper extends Mapper<Object, Text, Text, FloatWritable> {
        private boolean firstLine = true;
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if(firstLine){
                firstLine = false;
                return;
            }
            String[] fields = value.toString().split(";");

            String unit = fields[7];
            String year = fields[1];
            String category = fields[9];
            String flow = fields[4];
            String country = fields[0];
            float commodity_usd = Float.parseFloat(fields[5]);

            if(!Objects.equals(flow, "Export") || !Objects.equals(country, "Brazil")) return;
            context.write(verbose(unit, year, category), new FloatWritable(commodity_usd));
        }
    }

    public static class BackTransactionsReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        @Override
        protected void reduce(Text key, Iterable<FloatWritable> values, Reducer<Text, FloatWritable, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
            int count = 0;
            int total = 0;
            for(FloatWritable i : values){
                count += i.get();
                total += 1;
            }
            context.write(key, new FloatWritable((float) count / total));
        }
    }

    public static Text verbose(String unit, String year, String category){
        String value = "unit = " + unit + " Year = " + year + " Category = " + category;
        return new Text(value);
    }
}

