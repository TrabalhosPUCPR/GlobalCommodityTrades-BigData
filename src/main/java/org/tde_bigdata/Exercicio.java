package org.tde_bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public interface Exercicio {

    default Job setupJob(Configuration c) throws IOException {
        return new Job(c);
    }

    default void launch(Configuration c, Path input) throws IOException, InterruptedException, ClassNotFoundException {
        Job j = setupJob(c);
        FileInputFormat.addInputPath(j, input);
        j.waitForCompletion(false);
    }
}
