package ar.itba.edu.pod.hazel;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * Created by lumarzo on 16/11/16.
 */
public class Tuple implements DataSerializable{
    private int edad;
    private String analfabetismo;
    private String tipoVivienda;
    private String nombreDepartamento;
    private String nombreProvincia;
    private String hogarId;


    public Tuple() {
    }

    public Tuple(int edad, String analfabetismo, String tipoVivienda, String nombreDepartamento, String nombreProvincia, String hogarId) {
        this.edad = edad;
        this.analfabetismo = analfabetismo;
        this.tipoVivienda = tipoVivienda;
        this.nombreDepartamento = nombreDepartamento;
        this.nombreProvincia = nombreProvincia;
        this.hogarId = hogarId;
    }

    public int getEdad() {
        return edad;
    }

    public String getAnalfabetismo() {
        return analfabetismo;
    }

    public String getTipoVivienda() {
        return tipoVivienda;
    }

    public String getNombreDepartamento() {
        return nombreDepartamento;
    }

    public String getNombreProvincia() {
        return nombreProvincia;
    }

    public String getHogarId() {
        return hogarId;
    }

    public void setEdad(int edad) {
        this.edad = edad;
    }

    public void setAnalfabetismo(String analfabetismo) {
        this.analfabetismo = analfabetismo;
    }

    public void setTipoVivienda(String tipoVivienda) {
        this.tipoVivienda = tipoVivienda;
    }

    public void setNombreDepartamento(String nombreDepartamento) {
        this.nombreDepartamento = nombreDepartamento;
    }

    public void setNombreProvincia(String nombreProvincia) {
        this.nombreProvincia = nombreProvincia;
    }

    public void setHogarId(String hogarId) {
        this.hogarId = hogarId;
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeInt(edad);
        objectDataOutput.writeUTF(analfabetismo);
        objectDataOutput.writeUTF(tipoVivienda);
        objectDataOutput.writeUTF(nombreDepartamento);
        objectDataOutput.writeUTF(nombreProvincia);
        objectDataOutput.writeUTF(hogarId);
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        edad = objectDataInput.readInt();
        analfabetismo = objectDataInput.readUTF();
        tipoVivienda = objectDataInput.readUTF();
        nombreDepartamento = objectDataInput.readUTF();
        nombreProvincia = objectDataInput.readUTF();
        hogarId = objectDataInput.readUTF();
    }
}

;