package ar.itba.edu.pod.hazel;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.ArrayList;
import java.util.List;

public class Query5Reducer implements ReducerFactory<Integer, String, List<String>> {
    private static final long serialVersionUID = 1734879631198927649L;

    @Override
    public Reducer<String, List<String>> newReducer(final Integer cientosHab) {
        return new Reducer<String, List<String>>() {

            List<String> ans;
            List<String> deptos;

            @Override
            public void beginReduce() { // una sola vez en cada instancia
                ans = new ArrayList<>();
                deptos = new ArrayList<>();
            }

            @Override
            public void reduce(final String depto) {
                if (!deptos.isEmpty()){
                    for (String d: deptos) {
                        ans.add(String.format("%s + %s", d, depto));
                    }
                }
                deptos.add(depto);
            }

            @Override
            public List<String> finalizeReduce() {
                return ans;
            }
        };
    }
}
