package TDE1.Exercicio5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Exercicio5 {

    public static Job setupJob(Configuration c) throws IOException {
        Job job = new Job(c, "Ex5");
        Path output = new Path("output/outputEX5");
        job.setJarByClass(Exercicio5.class);
        job.setMapperClass(BackTransactionsMapper.class);
        job.setReducerClass(BackTransactionsReducer.class);
        job.setMapOutputKeyClass(MultiStringKeys.class);
        job.setMapOutputValueClass(AvgWritable.class);
        job.setOutputKeyClass(MultiStringKeys.class);
        job.setOutputValueClass(MultiStringKeys.class);
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

            double commodity_usd = Double.parseDouble(fields[5]);
            context.write(new MultiStringKeys(unit, year), new AvgWritable(commodity_usd, 1));
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

    public static class BackTransactionsReducer extends Reducer<MultiStringKeys, AvgWritable, MultiStringKeys, MultiStringKeys> {
        @Override
        protected void reduce(MultiStringKeys key, Iterable<AvgWritable> values, Context context) throws IOException, InterruptedException {
            double count = 0;
            int total = 0;
            double max = Double.MIN_VALUE;
            double min =  Double.MAX_VALUE;
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
            double mean = count / total;
            context.write(key, new MultiStringKeys(Double.toString(max), Double.toString(min), Double.toString(mean)));
        }
    }
}

