package ar.itba.edu.pod.hazel;

import com.hazelcast.mapreduce.Collator;

import java.util.*;

/**
 * Created by Lucas on 19/11/2016.
 */
public class Query5OrderCollator implements Collator<Map.Entry<Integer, List<String>>, List<Map.Entry<Integer, List<String>>>> {

    @Override
    public List<Map.Entry<Integer, List<String>>> collate(Iterable<Map.Entry<Integer, List<String>>> values) {
        List<Map.Entry<Integer, List<String>>> aux = new ArrayList<>();
        for (Map.Entry<Integer, List<String>> value : values) {
            if(!value.getValue().isEmpty())
                aux.add(value);
        }
        Collections.sort(aux,new ResultsComparator());
        return aux;
    }

    private static class ResultsComparator
            implements Comparator<Map.Entry<Integer, List<String>>> {

        @Override
        public int compare(Map.Entry<Integer, List<String>> o1, Map.Entry<Integer, List<String>> o2) {
            return o2.getKey().compareTo(o1.getKey());
        }
    }
}
