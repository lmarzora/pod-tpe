package ar.itba.edu.pod.hazel;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class CountReducer implements ReducerFactory<String, Integer, Integer> {
    private static final long serialVersionUID = 1734879631198927649L;

    @Override
    public Reducer<Integer, Integer> newReducer(final String word) {
        return new Reducer<Integer, Integer>() {
            private int sum;

            @Override
            public void beginReduce() { // una sola vez en cada instancia

                sum = 0;
            }

            @Override
            public void reduce(final Integer value) {
                sum += value;
            }

            @Override
            public Integer finalizeReduce() {
                System.out.println(String.format("Reduced for %s, %d", word, sum));
                return sum;
            }
        };
    }
}
