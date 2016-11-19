package ar.itba.edu.pod.hazel;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

// Parametrizar con los tipos de keyInput, ,valueInput, keyoutput, valueOutput
public class Query5Mapper implements Mapper<Integer, Tuple, String[], Integer> {
    private static final long serialVersionUID = -5535922778765480945L;

    @Override
    public void map(final Integer keyinput, final Tuple valueinput, final Context<String[], Integer> context) {
        String nombreDepto = valueinput.getNombreDepartamento();
        String nombreProv = valueinput.getNombreProvincia();
        context.emit(new String[]{nombreDepto,nombreProv}, 1);
    }
}
