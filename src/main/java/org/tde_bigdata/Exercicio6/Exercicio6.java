package org.tde_bigdata.Exercicio6;

import org.tde_bigdata.ContactenateMPs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;
import org.tde_bigdata.Exercicio5.AvgWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Objects;

public class Exercicio6 implements ContactenateMPs {

    public static Job setupJob(Configuration c) throws IOException, InterruptedException, ClassNotFoundException {
        Job job1 = new Job(c, "Ex6");
        Path input = new Path("in/transactions_amostra.csv");
        Path intermediate = new Path("output/intermediateEX6");
        Path output = new Path("output/outputEX6");
        job1.setJarByClass(Exercicio6.class);
        job1.setMapperClass(Exercicio6.MapEtapaA.class);
        job1.setReducerClass(Exercicio6.ReduceEtapaA.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(AvgWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job1, input);
        FileOutputFormat.setOutputPath(job1, intermediate);
        job1.waitForCompletion(false);

        Job job2 = new Job(c, "Ex62");
        job2.setJarByClass(Exercicio6.class);
        job2.setMapperClass(Exercicio6.MapEtapaB.class);
        job2.setReducerClass(Exercicio6.ReduceEtapaB.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(DoubleWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job2, intermediate);
        FileOutputFormat.setOutputPath(job2, output);
        job2.waitForCompletion(false);
        return job2;
    }

    public static class MapEtapaA extends Mapper<Object, Text, Text, AvgWritable> {
        private boolean firstLine = true;
        public void map(Object key, Text value, Context con)
                throws IOException, InterruptedException {
            if(firstLine){
                firstLine = false;
                return;
            }
            String[] fields = value.toString().split(";");
            String country = fields[0];
            double commodity_usd = Double.parseDouble(fields[5]);
            String flow = fields[4];
            if(!Objects.equals(flow, "Export")) return;
            con.write(new Text(country + ":"), new AvgWritable(commodity_usd, 1));
        }
    }

    public static class ReduceEtapaA extends Reducer<Text, AvgWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<AvgWritable> values, Context con)
                throws IOException, InterruptedException {
            double count = 0;
            int total = 0;
            for(AvgWritable i : values){
                count += i.getSomaValues();
                total += i.getN();
            }
            double mean = count / total;
            con.write(key, new DoubleWritable(mean));
        }
    }


    public static class MapEtapaB extends Mapper<Object, Text, Text, DoubleWritable> {
        private boolean firstLine = true;
        private double max = 0;
        public void map(Object key, Text value, Context con)
                throws IOException, InterruptedException {
            String[] fields = value.toString().split(":");
            System.out.println(Arrays.toString(fields));
            double mean = Double.parseDouble(fields[fields.length - 1]);
            con.write(new Text(fields[0]), new DoubleWritable(mean));
        }
    }

    public static class ReduceEtapaB extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        String country = "";
        double value = 0;
        public void reduce(Text key, Iterable<DoubleWritable> values, Context con)
                throws IOException, InterruptedException {

            for(DoubleWritable v : values){
                if (v.get() > value){
                    value = v.get();
                    country = key.toString();
                }
            }
            if(key.toString().equals("Zimbabwe")){
                con.write(new Text(country), new DoubleWritable(value));
            }
        }
    }
}
