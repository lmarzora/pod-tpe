package ar.itba.edu.pod.hazel;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

// Parametrizar con los tipos de keyInput, ,valueInput, keyoutput, valueOutput
public class Query3Mapper implements Mapper<Integer, Tuple, String, Integer> {
    private static final long serialVersionUID = -5535922778765480945L;

    @Override
    public void map(final Integer keyinput, final Tuple valueinput, final Context<String, Integer> context) {
        String nombreDepto = valueinput.getNombreDepartamento();
        Integer analfabeta = valueinput.getAnalfabetismo().equals("2")? 1:0;
        context.emit(nombreDepto, analfabeta);
    }
}
