package org.tde_bigdata.Exercicio7;

import com.sun.corba.se.spi.ior.ObjectKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.tde_bigdata.Exercicio2.StringDoubleKeys;
import org.tde_bigdata.Exercicio5.AvgWritable;
import org.tde_bigdata.Exercicio5.Exercicio5;
import org.tde_bigdata.Exercicio5.MultiStringKeys;

import java.io.IOException;

public class Exercicio7 {

    public static Job setupJob(Configuration c) throws IOException {
        Job job = new Job(c, "Ex7");
        Path output = new Path("output/outputEX7");
        job.setJarByClass(Exercicio7.class);
        job.setMapperClass(Exercicio7.Mapper1.class);

        job.setReducerClass(Exercicio5.BackTransactionsReducer.class);

        job.setMapOutputKeyClass(StringDoubleKeys.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileOutputFormat.setOutputPath(job, output);
        return job;
    }

    public static class Mapper1 extends Mapper<Object, Text, StringDoubleKeys, IntWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(";");
            if(fields[0].equals("country_or_area")) return;
            String year = fields[1];
            if(!year.equals("2016")) return;
            String commodityName = fields[3];
            String flowType = fields[4];
            context.write(new StringDoubleKeys(commodityName, flowType), new IntWritable(1));
        }
    }

    public static class Reducer1b extends Reducer<StringDoubleKeys, IntWritable, Text, IntWritable> {
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
