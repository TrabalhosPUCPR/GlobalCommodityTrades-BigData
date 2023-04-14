package org.example;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.BasicConfigurator;
import org.example.Exercicio1.Exercicio1;
import org.example.Exercicio2.Exercicio2;
import org.example.Exercicio5.Exercicio5;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public class Main {
    private static Path input = new Path("in/transactions_amostra.csv");
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();

        FileUtils.deleteDirectory(new File("output/")); // para nao precisar deletar na mao toda vez
        ArrayList<Job> exercicios = new ArrayList<>();

        exercicios.add(Exercicio1.setupJob(c));
        exercicios.add(Exercicio2.setupJob(c));
        exercicios.add(Exercicio5.setupJob(c));

        for(Job j : exercicios){
            launchJob(j);
        }

    }
    private static void launchJob(Job j) throws IOException, InterruptedException, ClassNotFoundException {
        FileInputFormat.addInputPath(j, input);
        j.waitForCompletion(false);
    }


}