package ar.itba.edu.pod.hazel;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class AverageReducer implements ReducerFactory<String, Integer[], Double> {
    private static final long serialVersionUID = 1734879631198927649L;

    @Override
    public Reducer<Integer[], Double> newReducer(final String word) {
        return new Reducer<Integer[], Double>() {
            private int sum;
            private int count;

            @Override
            public void beginReduce() { // una sola vez en cada instancia
                sum = 0;
                count = 0;
            }

            @Override
            public void reduce(final Integer[] value) {
                sum += value[0];
                count+= value[1];
            }

            @Override
            public Double finalizeReduce() {
                return ((double)sum)/count;
            }
        };
    }
}
