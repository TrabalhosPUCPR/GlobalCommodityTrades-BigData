package TDE1.Exercicio6;

import TDE1.GenericWritable;

public class DoubleStringKeys extends GenericWritable {

    public DoubleStringKeys(String string1, String string2){
        super(string1, string2);
    }
    public DoubleStringKeys(){}
    @Override
    public String toString() {
        return objects[0] + " " + objects[1];
    }

    public String getCountry(){
        return (String) objects[0];
    }

    public Double getValue(){
        return Double.parseDouble((String) objects[1]);
    }
}

