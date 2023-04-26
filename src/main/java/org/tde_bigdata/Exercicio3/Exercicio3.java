package org.tde_bigdata.Exercicio3;


import org.tde_bigdata.Exercicio;
import org.tde_bigdata.Exercicio2.StringDoubleKeys;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.tde_bigdata.Exercicio4.MultiStringKeys;
import org.tde_bigdata.Exercicio5.AvgWritable;

import java.io.IOException;
import java.util.Objects;


public class Exercicio3 implements Exercicio {
    @Override
    public Job setupJob(Configuration c) throws IOException {
        Job job = new Job(c, "Ex4");
        Path output = new Path("output/outputEX3");
        job.setJarByClass(Exercicio3.class);
        job.setMapperClass(BackTransactionsMapper.class);
        job.setReducerClass(BackTransactionsReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AvgWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setCombinerClass(Combiner.class);
        FileOutputFormat.setOutputPath(job, output);
        return job;
    }

    public static class BackTransactionsMapper extends Mapper<Object, Text, Text, AvgWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(";");

            if(fields[0].equals("country_or_area")) return;

            String year = fields[1];
            double commodity_usd = Double.parseDouble(fields[5]);

            context.write(new Text(year), new AvgWritable(commodity_usd, 1));
        }
    }

    public static class Combiner extends Reducer<Text, AvgWritable, Text, AvgWritable>{
        @Override
        protected void reduce(Text key, Iterable<AvgWritable> values, Context context) throws IOException, InterruptedException {
            double count = 0;
            int total = 0;
            for(AvgWritable i : values){
                count += i.getSomaValues();
                total += i.getN();
            }
            context.write(key, new AvgWritable(count,total));
        }
    }

    public static class BackTransactionsReducer extends Reducer<Text, AvgWritable, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<AvgWritable> values, Context context) throws IOException, InterruptedException {
            double count = 0;
            int total = 0;
            for(AvgWritable i : values){
                count += i.getSomaValues();
                total += i.getN();
            }
            context.write(new Text(key.toString()), new DoubleWritable( count / total));
        }
    }
}


