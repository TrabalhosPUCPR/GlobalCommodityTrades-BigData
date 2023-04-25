package TDE1.Exercicio4;

import TDE1.GenericWritable;

public class MultiStringKeys extends GenericWritable{

        public MultiStringKeys(String string1, String string2, String string3){
            super(string1, string2, string3);
        }

        public MultiStringKeys(){

        }
        @Override
        public String toString() {
            return objects[0] + " " + objects[1] + " " + objects[2];
        }



}
