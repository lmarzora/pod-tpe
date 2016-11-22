package ar.itba.edu.pod.hazel;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;


public class Pair<T,V> implements DataSerializable{
    private T first;
    private V second;

    public Pair() {
    }

    public Pair(T first, V second) {
        this.first = first;
        this.second = second;
    }

    public T getFirst() {
        return first;
    }

    public void setFirst(T first) {
        this.first = first;
    }

    public V getSecond() {
        return second;
    }

    public void setSecond(V second) {
        this.second = second;
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeObject(first);
        objectDataOutput.writeObject(second);
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        first = objectDataInput.readObject();
        second = objectDataInput.readObject();
    }
}

;