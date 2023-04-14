package org.tde_bigdata.Exercicio5;

import org.tde_bigdata.GenericWritable;

public class AvgWritable extends GenericWritable {
    public double getSomaValues() {
        return Double.parseDouble(objects[0].toString());
    }

    public void setSomaValues(double somaValues) {
        objects[0] = somaValues;
    }

    public int getN() {
        return Integer.parseInt(objects[1].toString());
    }

    public void setN(int n) {
        objects[1] = n;
    }

    public AvgWritable() {}

    public AvgWritable(double somaValues, int n) {
        super(somaValues, n);
    }
}

