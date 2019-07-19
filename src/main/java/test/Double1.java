package test;

import org.apache.avro.LogicalTypes;

import java.math.BigDecimal;
import java.math.BigInteger;

public class Double1 {

    public static void main(String[] args) {
        Double a= 0.1;
        Double b= 0.2;
        Double c = a+b;
        System.out.println("c="+(c == 0.3));

        BigDecimal d=new BigDecimal(0.1);
        BigDecimal e=new BigDecimal(0.2);
        BigDecimal g=new BigDecimal(0.3);

        BigDecimal f=d.add(e);
        System.out.println(f);
        System.out.println(e);
        System.out.println(d);


    }
}
