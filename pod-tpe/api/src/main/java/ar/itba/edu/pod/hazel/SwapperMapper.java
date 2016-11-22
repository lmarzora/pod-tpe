package ar.itba.edu.pod.hazel;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

// Parametrizar con los tipos de keyInput, ,valueInput, keyoutput, valueOutput
public class SwapperMapper implements Mapper<String, Integer, Integer, String> {
    private static final long serialVersionUID = -5535922778765480945L;

    @Override
    public void map(final String keyinput, final Integer valueinput, final Context<Integer, String> context) {
        context.emit(((valueinput/100))*100, keyinput);
    }
}
