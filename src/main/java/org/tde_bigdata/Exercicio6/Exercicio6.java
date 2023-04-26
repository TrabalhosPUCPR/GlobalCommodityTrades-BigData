package org.tde_bigdata.Exercicio6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.tde_bigdata.Exercicio;
import org.tde_bigdata.Exercicio5.AvgWritable;

import java.io.IOException;
import java.util.Objects;

public class Exercicio6 implements Exercicio {

    private Job job1, job2;

    public Exercicio6(Configuration c) throws IOException {
        setupJob(c);
    }

    @Override
    public Job setupJob(Configuration c) throws IOException {
        job1 = new Job(c, "Ex6");
        Path input = new Path("in/transactions_amostra.csv");
        Path intermediate = new Path("output/intermediate/outputEX6");
        Path output = new Path("output/outputEX6");
        job1.setJarByClass(Exercicio6.class);
        job1.setMapperClass(MapEtapaA.class);
        job1.setReducerClass(ReduceEtapaA.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(AvgWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);
        job1.setCombinerClass(CombinerA.class);
        FileInputFormat.addInputPath(job1, input);
        FileOutputFormat.setOutputPath(job1, intermediate);

        job2 = new Job(c, "Ex62");
        job2.setJarByClass(Exercicio6.class);
        job2.setMapperClass(MapEtapaB.class);
        job2.setReducerClass(ReduceEtapaB.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(DoubleStringKeys.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleStringKeys.class);
        job2.setCombinerClass(CombinerB.class);
        FileInputFormat.addInputPath(job2, intermediate);
        FileOutputFormat.setOutputPath(job2, output);
        return null;
    }

    @Override
    public void launch(Configuration c, Path input) throws IOException, InterruptedException, ClassNotFoundException {
        FileInputFormat.addInputPath(job1, input);
        job1.waitForCompletion(false);
        job2.waitForCompletion(false);
    }

    public static class MapEtapaA extends Mapper<Object, Text, Text, AvgWritable> {
        public void map(Object key, Text value, Context con)
                throws IOException, InterruptedException {
            String[] fields = value.toString().split(";");
            if(fields[0].equals("country_or_area")) return;
            String country = fields[0];
            double commodity_usd = Double.parseDouble(fields[5]);
            String flow = fields[4];
            if(!Objects.equals(flow, "Export")) return;
            con.write(new Text(country + ":"), new AvgWritable(commodity_usd, 1));
        }
    }

    public static class CombinerA extends Reducer<Text, AvgWritable, Text, AvgWritable>{
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


    public static class MapEtapaB extends Mapper<Object, Text, Text, DoubleStringKeys> {
        public void map(Object key, Text value, Context con)
                throws IOException, InterruptedException {
            String[] fields = value.toString().split(":");
            con.write(new Text("global"), new DoubleStringKeys(fields[0],fields[fields.length - 1]));
        }
    }

    public static class CombinerB extends Reducer<Text, DoubleStringKeys, Text, DoubleStringKeys>{
        @Override
        protected void reduce(Text key, Iterable<DoubleStringKeys> values, Context context) throws IOException, InterruptedException {
            String country = "";
            double value = 0;
            for(DoubleStringKeys v : values){
                if (v.getValue() > value){
                    country = v.getCountry();
                    value = v.getValue();
                }
            }
            context.write(new Text("global"), new DoubleStringKeys(country, Double.toString(value)));
        }
    }

    public static class ReduceEtapaB extends Reducer<Text, DoubleStringKeys, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleStringKeys> values, Context con)
                throws IOException, InterruptedException {
            String country = "";
            double value = 0;
            for(DoubleStringKeys v : values){
                if (v.getValue() > value){
                    country = v.getCountry();
                    value = v.getValue();
                }
            }
            con.write(new Text(country), new DoubleWritable(value));
        }
    }
}
