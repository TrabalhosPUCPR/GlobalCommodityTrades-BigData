package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Objects;


public class Exercicio5 {

    public static Job setupJob(Configuration c) throws IOException {
        Job job = new Job(c, "Ex5");
        Path output = new Path("output/outputEX5");
        job.setJarByClass(Exercicio5.class);
        job.setMapperClass(BackTransactionsMapper.class);
        job.setReducerClass(BackTransactionsReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AvgWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(AvgWritable.class);
        FileOutputFormat.setOutputPath(job, output);
        return job;
    }

    public static class BackTransactionsMapper extends Mapper<Object, Text, Text, AvgWritable> {
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

            double commodity_usd = Double.parseDouble(fields[5]);
            context.write(verbose(unit, year), new AvgWritable(commodity_usd, 1));
        }
    }

    public static class BackTransactionsReducer extends Reducer<Text, AvgWritable, Text, Text> {
        //private boolean firstProcess = true;
        @Override
        protected void reduce(Text key, Iterable<AvgWritable> values, Reducer<Text, AvgWritable, Text, Text>.Context context) throws IOException, InterruptedException {
            double count = 0;
            int total = 0;
            Double max = Double.MIN_VALUE;
            Double min =  Double.MAX_VALUE;
            for(AvgWritable i : values){
                count += i.getSomaValues();
                total += i.getN();
                if(i.getSomaValues() > max){
                    max = i.getSomaValues();
                }
                if(i.getSomaValues() < min){
                    min = i.getSomaValues();
                }
            }


            Double mean = count / total;
            context.write(key, Answer( max.toString(), min.toString(), mean.toString()));
            //firstProcess = true;
        }
    }

    public static Text verbose(String unit, String year){
        String value = "unit = " + unit + " Year = " + year;
        return new Text(value);
    }

    public static Text Answer(String max, String min, String avg){
        String value = "max = " + max + " min = " + min + " avg = " + avg;
        return new Text(value);
    }
}

