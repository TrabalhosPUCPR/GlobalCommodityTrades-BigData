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


public class Exercicio5 {

    public static Job setupJob(Configuration c) throws IOException {
        Job job = new Job(c, "Ex5");
        Path output = new Path("output/outputEX5");
        job.setJarByClass(Exercicio5.class);
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

            float commodity_usd = Float.parseFloat(fields[5]);
            context.write(verbose(unit, year), new FloatWritable(commodity_usd));
        }
    }

    public static class BackTransactionsReducer extends Reducer<Text, FloatWritable, Text, Text> {
        private boolean firstProcess = true;
        @Override
        protected void reduce(Text key, Iterable<FloatWritable> values, Reducer<Text, FloatWritable, Text, Text>.Context context) throws IOException, InterruptedException {
            int count = 0;
            int total = 0;
            Float max = (float) 0.0;
            Float min = (float) 0.0;
            for(FloatWritable i : values){
                count += i.get();
                total += 1;
                if(firstProcess){
                    firstProcess = false;
                    min = i.get();
                    max = i.get();
                }else{
                    if(i.get() > max){
                        max = i.get();
                    }else if(min > i.get()){
                        min = i.get();
                    }
                }

            }
            Float mean = (float) count / total;


            context.write(key, Answer( max.toString(), min.toString(), mean.toString()));
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


