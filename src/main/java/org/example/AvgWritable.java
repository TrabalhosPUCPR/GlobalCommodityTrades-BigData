package org.example;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class AvgWritable implements WritableComparable<AvgWritable> {

    private double somaValues;
    private int n;

    public double getSomaValues() {
        return somaValues;
    }

    public void setSomaValues(double somaValues) {
        this.somaValues = somaValues;
    }

    public int getN() {
        return n;
    }

    public void setN(int n) {
        this.n = n;
    }

    public AvgWritable() {
    }

    public AvgWritable(double somaValues, int n) {
        this.somaValues = somaValues;
        this.n = n;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AvgWritable that = (AvgWritable) o;
        return Double.compare(that.somaValues, somaValues) == 0 && n == that.n;
    }

    @Override
    public int hashCode() {
        return Objects.hash(somaValues, n);
    }

    @Override
    public int compareTo(AvgWritable o) {
        return Integer.compare(this.hashCode(), o.hashCode());
    }

    @Override
    public String toString() {
        return  somaValues + "," + n;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(somaValues);
        dataOutput.writeInt(n);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        // ATENÇÃO: USAR A MESMA ORDEM NO WRITE
        somaValues = dataInput.readDouble();
        n = dataInput.readInt();
    }
}

