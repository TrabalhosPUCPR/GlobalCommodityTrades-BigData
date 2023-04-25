package TDE1.Exercicio4;


import TDE1.Exercicio2.StringDoubleKeys;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import TDE1.Exercicio5.AvgWritable;

import java.io.IOException;
import java.util.Objects;


public class Exercicio4 {

    public static Job setupJob(Configuration c) throws IOException {
        Job job = new Job(c, "Ex4");
        Path output = new Path("output/outputEX4");
        job.setJarByClass(Exercicio4.class);
        job.setMapperClass(BackTransactionsMapper.class);
        job.setReducerClass(BackTransactionsReducer.class);

        job.setMapOutputKeyClass(MultiStringKeys.class);
        job.setMapOutputValueClass(AvgWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setCombinerClass(Combiner.class);
        FileOutputFormat.setOutputPath(job, output);
        return job;
    }

    public static class BackTransactionsMapper extends Mapper<Object, Text, MultiStringKeys, AvgWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(";");

            if(fields[0].equals("country_or_area")) return;

            String unit = fields[7];
            String year = fields[1];
            String category = fields[9];
            String flow = fields[4];
            String country = fields[0];
            double commodity_usd = Double.parseDouble(fields[5]);

            if(!Objects.equals(flow, "Export") || !Objects.equals(country, "Brazil")) return;
            context.write(new MultiStringKeys(unit, year, category), new AvgWritable(commodity_usd, 1));
        }
    }

    public static class Combiner extends Reducer<MultiStringKeys, AvgWritable, MultiStringKeys, AvgWritable>{
        @Override
        protected void reduce(MultiStringKeys key, Iterable<AvgWritable> values, Context context) throws IOException, InterruptedException {
            double count = 0;
            int total = 0;
            for(AvgWritable i : values){
                count += i.getSomaValues();
                total += i.getN();
            }

            context.write(key, new AvgWritable(count,total));
        }
    }

    public static class BackTransactionsReducer extends Reducer<MultiStringKeys, AvgWritable, Text, DoubleWritable> {
        @Override
        protected void reduce(MultiStringKeys key, Iterable<AvgWritable> values, Reducer<MultiStringKeys, AvgWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
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


