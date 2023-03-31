package org.example;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();

        FileUtils.deleteDirectory(new File("output/")); // para nao precisar deletar na mao toda vez

        ArrayList<Job> exercicios = new ArrayList<>();

        // SETUP EX 1
        Job job1 = new Job(c, "Ex1");
        Path input = new Path("in/transactions_amostra.csv/transactions_amostra.csv");
        Path output = new Path("output/outputEX1.txt");
        job1.setJarByClass(Main.class);
        job1.setMapperClass(BackTransactionsMapperEX1.class);
        job1.setReducerClass(BackTransactionsReducerEX1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);


        FileOutputFormat.setOutputPath(job1, output);

        exercicios.add(job1);

        for(Job j : exercicios){
            FileInputFormat.addInputPath(j, input);
            j.waitForCompletion(false);
        }

    }

    public static class BackTransactionsMapperEX1 extends Mapper<Object, Text, Text, IntWritable> {
        private boolean jafoi = false;
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if(!jafoi){
                jafoi = true;
                return;
            }
            String country = value.toString().split(";")[0];
            if(!Objects.equals(country, "Brazil")) return;
            context.write(new Text(country), new IntWritable(1));
        }
    }

    public static class BackTransactionsReducerEX1 extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int count = 0;
            for(IntWritable i : values){
                count += i.get();
            }
            context.write(key, new IntWritable(count));
        }
    }
}