package ar.itba.edu.pod.hazel;

import java.util.StringTokenizer;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

// Parametrizar con los tipos de keyInput, ,valueInput, keyoutput, valueOutput
public class Query1Mapper implements Mapper<Integer, Tuple, String, Integer> {
    private static final long serialVersionUID = -5535922778765480945L;

    @Override
    public void map(final Integer keyinput, final Tuple valueinput, final Context<String, Integer> context) {
        int edad = valueinput.getEdad();
        if (edad < 0)
            return;
        if (edad <= 14)
            context.emit("0-14",1);
        else if (edad <= 64)
            context.emit("15-64",1);
        else
            context.emit("65-?",1);
    }
}
