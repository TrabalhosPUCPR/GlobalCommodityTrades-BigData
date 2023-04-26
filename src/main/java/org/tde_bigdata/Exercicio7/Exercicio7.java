package org.tde_bigdata.Exercicio7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.tde_bigdata.Exercicio;
import org.tde_bigdata.Exercicio2.StringDoubleKeys;
import org.tde_bigdata.Exercicio5.MultiStringKeys;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class Exercicio7 implements Exercicio {
    private Job job1, job2;

    public Exercicio7(Configuration c) throws IOException {
        setupJob(c);
    }

    public Job setupJob(Configuration c) throws IOException {
        Path output = new Path("output/outputEX7");
        Path outputIntermediate = new Path("output/intermediate/outputEX7");

        job1 = new Job(c, "Ex7");
        job1.setJarByClass(Exercicio7.class);

        job1.setMapperClass(Mapper1.class);
        job1.setCombinerClass(Combiner1.class);
        job1.setReducerClass(Reducer1.class);

        job1.setMapOutputKeyClass(MultiStringKeys.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileOutputFormat.setOutputPath(job1, outputIntermediate);

        job2 = new Job(c);
        job2.setJarByClass(Exercicio7.class);

        job2.setMapperClass(Mapper2.class);
        job2.setCombinerClass(Combiner2.class);
        job2.setReducerClass(Reducer2.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(CommodityWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(MultiStringKeys.class);

        FileInputFormat.addInputPath(job2, outputIntermediate);
        FileOutputFormat.setOutputPath(job2, output);
        return null;
    }

    @Override
    public void launch(Configuration c, Path input) throws IOException, InterruptedException, ClassNotFoundException {
        FileInputFormat.addInputPath(job1, input);
        job1.waitForCompletion(false);
        job2.waitForCompletion(false);
    }

    public static class Mapper1 extends Mapper<Object, Text, MultiStringKeys, IntWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(";");
            if(line[0].equals("country_or_area") || !line[1].equals("2016")) return;

            String flow = line[4];
            String commodity = line[3];
            int quantity = (int) Double.parseDouble(line[8]);
            MultiStringKeys writable = new MultiStringKeys(flow, commodity);
            context.write(writable, new IntWritable(quantity));
        }
    }

    public static class Combiner1 extends Reducer<MultiStringKeys, IntWritable, MultiStringKeys, IntWritable>{
        @Override
        protected void reduce(MultiStringKeys key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for(IntWritable i : values){
                count += i.get();
            }
            context.write(key, new IntWritable(count));
        }
    }

    public static class Reducer1 extends Reducer<MultiStringKeys, IntWritable, MultiStringKeys, IntWritable> {
        @Override
        protected void reduce(MultiStringKeys key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            key.splitter = ";;;;;";
            int quantity = 0;
            for (IntWritable i : values){
                quantity += i.get();
            }
            context.write(key, new IntWritable(quantity));
        }
    }

    public static class Mapper2 extends Mapper<Object, Text, Text, CommodityWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(";;;;;");
            String flow = line[0];
            String commodity = line[1];
            int quantity = Integer.parseInt(line[2].replace("\t", ""));
            context.write(new Text(flow), new CommodityWritable(commodity, quantity));
        }
    }

    public static class Combiner2 extends Reducer<Text, CommodityWritable, Text, CommodityWritable>{
        @Override
        protected void reduce(Text key, Iterable<CommodityWritable> values, Context context) throws IOException, InterruptedException {
            int max = Integer.MIN_VALUE;
            CommodityWritable highestCommodity = null;
            for(CommodityWritable cw : values){
                int quantity = cw.getQuantity();
                if(quantity > max){
                    max = quantity;
                    highestCommodity = new CommodityWritable(cw);
                }
            }
            context.write(key, highestCommodity);
        }
    }

    public static class Reducer2 extends Reducer<Text, CommodityWritable, Text, MultiStringKeys> {
        @Override
        protected void reduce(Text key, Iterable<CommodityWritable> values, Context context) throws IOException, InterruptedException {
            int max = Integer.MIN_VALUE;
            CommodityWritable c = new CommodityWritable();
            for(CommodityWritable cw : values){
                int quantity = cw.getQuantity();
                if(quantity > max){
                    max = quantity;
                    c = new CommodityWritable(cw);
                }
            }
            context.write(key, new MultiStringKeys(" Name:", c.getCommodityName(), " Quantity:",String.valueOf(c.getQuantity())));
        }
    }
}
