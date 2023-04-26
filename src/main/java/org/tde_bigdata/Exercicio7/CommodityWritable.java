package org.tde_bigdata.Exercicio7;

import org.tde_bigdata.GenericWritable;

public class CommodityWritable extends GenericWritable {

    public CommodityWritable(String commodityName, int quantity){
        super(commodityName, quantity);
    }
    public CommodityWritable(CommodityWritable cw){
        super(cw.objects);
    }
    public CommodityWritable(){}

    public String getCommodityName(){
        return objects[0].toString();
    }
    public int getQuantity(){
        return Integer.parseInt(objects[1].toString());
    }

}
