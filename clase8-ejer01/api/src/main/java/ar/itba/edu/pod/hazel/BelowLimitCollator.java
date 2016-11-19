package ar.itba.edu.pod.hazel;

import com.hazelcast.mapreduce.Collator;

import java.util.*;

/**
 * Created by Lucas on 19/11/2016.
 */
public class BelowLimitCollator implements Collator<Map.Entry<String, Integer>, List<Map.Entry<String, Integer>>> {

    int limit;

    public BelowLimitCollator(int limit){
        this.limit = limit;
    }

    @Override
    public List<Map.Entry<String, Integer>> collate(Iterable<Map.Entry<String, Integer>> values) {
        List<Map.Entry<String, Integer>> aux = new ArrayList<>();
        for (Map.Entry<String, Integer> value : values) {
            if(value.getValue()<limit)
                aux.add(value);
        }
        Collections.sort(aux,new IntegerComparator());
        return aux;
    }

    private static class IntegerComparator
            implements Comparator<Map.Entry<String, Integer>> {

        @Override
        public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
            return Integer.compare(o2.getValue(), o1.getValue());
        }
    }
}
