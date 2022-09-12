package com.flinklearn.batch.chapter3;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;

/*Takes a Tuple7 as input and returns a Tuple8 as output */

public class MapComputeTotalOrderValue implements MapFunction<Tuple7<Integer, String, String, String, Integer, Double,
        String>,
        Tuple8<Integer, String, String, String, Integer, Double, String, Double>> {

    @Override
    public Tuple8<Integer, String, String, String, Integer, Double, String, Double>
    map(Tuple7<Integer, String, String, String, Integer, Double, String> order) {
        return new Tuple8<>(order.f0, order.f1, order.f2, order.f3, order.f4, order.f5, order.f6,
                (order.f4 * order.f5));
    }
}

