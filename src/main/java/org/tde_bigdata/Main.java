package org.tde_bigdata;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.BasicConfigurator;
import org.tde_bigdata.Exercicio1.Exercicio1;
import org.tde_bigdata.Exercicio2.Exercicio2;
import org.tde_bigdata.Exercicio3.Exercicio3;
import org.tde_bigdata.Exercicio4.Exercicio4;
import org.tde_bigdata.Exercicio5.Exercicio5;
import org.tde_bigdata.Exercicio6.Exercicio6;
import org.tde_bigdata.Exercicio7.Exercicio7;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public class Main {
    private static final Path input = new Path("in/transactions_amostra.csv");
    public static void main(String[] args) throws IOException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();

        FileUtils.deleteDirectory(new File("output/")); // para nao precisar deletar na mao toda vez
        ArrayList<Exercicio> exercicios = new ArrayList<>();

        exercicios.add(new Exercicio1());
        exercicios.add(new Exercicio2());
        exercicios.add(new Exercicio3());
        exercicios.add(new Exercicio4());
        exercicios.add(new Exercicio5());
        exercicios.add(new Exercicio6(c));
        exercicios.add(new Exercicio7(c));

        int exercicio = 1;
        for(Exercicio e : exercicios){
            try{
                e.launch(c, input);
            }catch (Exception ex){
                System.err.println("\n\nERRO NO EXERCICIO " + exercicio + ":\n");
                ex.printStackTrace();
                return;
            }
            exercicio++;
        }
    }


}