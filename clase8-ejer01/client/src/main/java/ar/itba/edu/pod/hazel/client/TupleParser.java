package ar.itba.edu.pod.hazel.client;

import ar.itba.edu.pod.hazel.Tuple;

/**
 * Created by lumarzo on 16/11/16.
 */
public class TupleParser {

    public static Tuple parse(String s) {
        String[] data = s.split(",");
        return new Tuple(Integer.valueOf(data[3]), data[4], data[0], data[6], data[7], data[8]);
    }
}
