package ar.itba.edu.pod.hazel;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.HashSet;
import java.util.Set;

public class Query2Reducer implements ReducerFactory<String, Pair<String,Integer>, Double> {
    private static final long serialVersionUID = 1734879631198927649L;

    @Override
    public Reducer<Pair<String,Integer>, Double> newReducer(final String tipoVivienda) {
        return new Reducer<Pair<String, Integer>, Double>() {
            private int sum;
            private Set<String> hogarIDs;

            @Override
            public void beginReduce() { // una sola vez en cada instancia
                hogarIDs = new HashSet<>();
                sum = 0;
            }

            @Override
            public void reduce(final Pair<String, Integer> value) {
                sum += value.getSecond();
                hogarIDs.add(value.getFirst());
            }

            @Override
            public Double finalizeReduce() {
                return ((double)sum)/hogarIDs.size();
            }
        };
    }
}
