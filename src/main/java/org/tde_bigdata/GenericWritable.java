package org.tde_bigdata;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public abstract class GenericWritable implements WritableComparable<GenericWritable> {

    public Object[] objects;
    public GenericWritable(Object... objects){
        this.objects = objects;
    }
    public GenericWritable(){}

    @Override
    public String toString(){
        StringBuilder s = new StringBuilder();
        for(Object o : objects){
            s.append(o);
            s.append(" ");
        }
        return s.toString();
    }

    @Override
    public boolean equals(Object o) {
        if(o.getClass() != this.getClass()) return false;
        return this.hashCode() == o.hashCode();
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(objects);
    }

    @Override
    public int compareTo(GenericWritable genericWritable) {
        return Integer.compare(this.hashCode(), genericWritable.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        StringBuilder s = new StringBuilder();
        for (Object o : objects){
            s.append(o.toString());
            s.append(";;");
        }
        dataOutput.writeUTF(s.toString());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.objects = dataInput.readUTF().split(";;");
    }
}
