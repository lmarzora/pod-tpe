package ar.itba.edu.pod.hazel;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

// Parametrizar con los tipos de keyInput, ,valueInput, keyoutput, valueOutput
public class Query2Mapper implements Mapper<Integer, Tuple, String, Pair<String,Integer>> {
    private static final long serialVersionUID = -5535922778765480945L;

    @Override
    public void map(final Integer keyinput, final Tuple valueinput, final Context<String, Pair<String,Integer>> context) {
        String hogarID = valueinput.getHogarId();
        String tipoVivienda = valueinput.getTipoVivienda();
        context.emit(tipoVivienda, new Pair<>(hogarID,1));
    }
}
