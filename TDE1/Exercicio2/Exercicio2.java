package TDE1.Exercicio2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Exercicio2 {

    public static Job setupJob(Configuration c) throws IOException {
        Job job = new Job(c, "Ex2");
        Path output = new Path("output/outputEX2");
        job.setJarByClass(Exercicio2.class);
        job.setMapperClass(BackTransactionsMapper.class);
        job.setReducerClass(BackTransactionsReducer.class);

        job.setMapOutputKeyClass(StringDoubleKeys.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setCombinerClass(Combiner.class);

        FileOutputFormat.setOutputPath(job, output);
        return job;
    }

    public static class BackTransactionsMapper extends Mapper<Object, Text, StringDoubleKeys, IntWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(";");
            if(fields[0].equals("country_or_area")) return;
            String year = fields[1];
            String flow = fields[4];
            context.write(new StringDoubleKeys(year, flow), new IntWritable(1));
        }
    }

    public static class Combiner extends Reducer<StringDoubleKeys, IntWritable, StringDoubleKeys, IntWritable>{
        @Override
        protected void reduce(StringDoubleKeys key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for(IntWritable i : values){
                count += i.get();
            }
            context.write(key, new IntWritable(count));
        }
    }

    public static class BackTransactionsReducer extends Reducer<StringDoubleKeys, IntWritable, Text, IntWritable> {
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
