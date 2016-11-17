package ar.itba.edu.pod.hazel.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import ar.itba.edu.pod.hazel.Tuple;
import ar.itba.edu.pod.hazel.CountCombiner;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import ar.itba.edu.pod.hazel.Query1Mapper;
import ar.itba.edu.pod.hazel.CountReducer;

public class TPEClient {
    private static final String MAP_NAME = "censo";

    // el directorio wc dentro en un directorio llamado "resources"
    // al mismo nivel que la carpeta src, etc.
    private static final String FILE = "files/dataset-1000.csv";

    public static void main(final String[] args)
            throws InterruptedException, ExecutionException, IOException, URISyntaxException {
        final String name = System.getProperty("name");
        String pass = System.getProperty("pass");
        if (pass == null) {
            pass = "dev-pass";
        }
        System.out.println(String.format("Connecting with cluster dev-name [%s]", name));

        final ClientConfig ccfg = new ClientConfig();
        ccfg.getGroupConfig().setName(name).setPassword(pass);

        // no hay descubrimiento automatico,
        // pero si no decimos nada intentará usar LOCALHOST
        final String addresses = System.getProperty("addresses");
        if (addresses != null) {
            final String[] arrayAddresses = addresses.split("[,;]");
            final ClientNetworkConfig net = new ClientNetworkConfig();
            net.addAddress(arrayAddresses);
            ccfg.setNetworkConfig(net);
        }
        final HazelcastInstance client = HazelcastClient.newHazelcastClient(ccfg);

        System.out.println(client.getCluster());

        // Preparar la particion de datos y distribuirla en el cluster a traves
        // del IMap
        final IMap<Integer, Tuple> map = client.getMap(MAP_NAME);


        System.out.println("Loading Map");
        final InputStream is = TPEClient.class.getClassLoader().getResourceAsStream(FILE);

        final LineNumberReader reader = new LineNumberReader(new InputStreamReader(is));
        reader.readLine();
        String line = null;
        while ((line = reader.readLine()) != null) {
            map.put(reader.getLineNumber(), TupleParser.parse(line));
        }

        System.out.println("Map loaded");

        // Ahora el JobTracker y los Workers!
        System.out.println("Getting traker");
        final JobTracker tracker = client.getJobTracker("default");

        // Ahora el Job desde los pares(key, Value) que precisa MapReduce
        System.out.println("Getting source");
        final KeyValueSource<Integer, Tuple> source = KeyValueSource.fromMap(map);

        System.out.println("New job");
        final Job<Integer, Tuple> job = tracker.newJob(source);

        // Orquestacion de Jobs y lanzamiento
        System.out.println("Sending job");
        final ICompletableFuture<Map<String, Integer>> future = job
                                                             .mapper(new Query1Mapper())
                                                             .combiner(new CountCombiner())
                                                             .reducer(new CountReducer())
                                                             .submit();

        // Tomar resultado e Imprimirlo
        final Map<String, Integer> a = future.get();

        System.out.println(a);

        System.exit(0);
    }
}
