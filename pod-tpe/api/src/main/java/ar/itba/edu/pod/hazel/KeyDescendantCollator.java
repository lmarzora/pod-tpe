package ar.itba.edu.pod.hazel;

import com.hazelcast.mapreduce.Collator;

import java.util.*;

/**
 * Created by Lucas on 19/11/2016.
 */
public class KeyDescendantCollator<T> implements Collator<Map.Entry<String, T>, List<Map.Entry<String, T>>> {

    @Override
    public List<Map.Entry<String, T>> collate(Iterable<Map.Entry<String, T>> values) {
        List<Map.Entry<String, T>> aux = new ArrayList<>();
        for (Map.Entry<String, T> value : values) {
            aux.add(value);
        }
        Collections.sort(aux,new StringComparator<>());
        return aux;
    }

    private static class StringComparator<T>
            implements Comparator<Map.Entry<String, T>> {

        @Override
        public int compare(Map.Entry<String, T> o1, Map.Entry<String, T> o2) {
            return o1.getKey().compareTo(o2.getKey());
        }
    }
}
