package org.tde_bigdata.Exercicio2;

import org.tde_bigdata.GenericWritable;

public class StringDoubleKeys extends GenericWritable {

    public StringDoubleKeys(String string1, String string2){
        super(string1, string2);
    }
    public StringDoubleKeys(){}
    @Override
    public String toString() {
        return objects[0] + " " + objects[1];
    }
}
