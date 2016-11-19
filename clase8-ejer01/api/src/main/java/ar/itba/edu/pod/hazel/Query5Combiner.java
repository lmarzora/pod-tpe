package ar.itba.edu.pod.hazel;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

/**
 * Created by lumarzo on 02/11/16.
 */
public class Query5Combiner implements CombinerFactory<String[], Integer, Integer> {
    @Override
    public Combiner<Integer, Integer> newCombiner(String[] s) {
        return new Combiner<Integer, Integer>() {
            private int count = 0;
            @Override
            public void combine(Integer value) {
                count = count + value;
            }

            @Override
            public Integer finalizeChunk() {
                return count;
            }

            public void reset() {
                count = 0;
            }
        };
    }
}
