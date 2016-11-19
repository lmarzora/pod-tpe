package ar.itba.edu.pod.hazel.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import ar.itba.edu.pod.hazel.*;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

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

        String queryNumber = System.getProperty("query");

        System.out.println("query number:" + queryNumber);
        switch (queryNumber){
            case "1":
                final ICompletableFuture<Map<String, Integer>> future1 = job
                        .mapper(new Query1Mapper())
                        .combiner(new CountCombiner())
                        .reducer(new CountReducer())
                        .submit();

                // Tomar resultado e Imprimirlo
                final Map<String, Integer> ans1 = future1.get();

                query1printer(ans1);
                break;

            case "2":
                final ICompletableFuture<Map<String,Double>> future2 = job
                        .mapper(new Query2Mapper())
                        .reducer(new Query2Reducer())
                        .submit();

                // Tomar resultado e Imprimirlo
                final Map<String, Double> ans2 = future2.get();

                query2printer(ans2);
                break;

            case "3":
                int n = Integer.valueOf(System.getProperty("n","5"));
                final ICompletableFuture<List<Map.Entry<String,Double>>> future3 = job
                        .mapper(new Query3Mapper())
                        .combiner(new AverageCombiner())
                        .reducer(new AverageReducer())
                        .submit(new TopNCollator(n));

                // Tomar resultado e Imprimirlo
                final List<Map.Entry<String,Double>> ans3 = future3.get();

                query3printer(ans3);
                break;

            case "4":
                String prov = System.getProperty("prov","Buenos Aires");
                int tope = Integer.valueOf(System.getProperty("tope","500"));
                final ICompletableFuture<List<Map.Entry<String,Integer>>> future4 = job
                        .mapper(new Query4Mapper(prov))
                        .combiner(new CountCombiner())
                        .reducer(new CountReducer())
                        .submit(new BelowLimitCollator(tope));

                // Tomar resultado e Imprimirlo
                final List<Map.Entry<String,Integer>> ans4 = future4.get();

                query4printer(ans4);
                break;

            case "5":
                final ICompletableFuture<Map<String[], Integer>> future5_1 = job
                        .mapper(new Query5Mapper())
                        .combiner(new Query5Combiner())
                        .reducer(new Query5Reducer())
                        .submit();

                // Tomar resultado como base para segundo map reduce
                final Map<String[],Integer> habitantesPorDepto = future5_1.get();

                query5printer(habitantesPorDepto);      //TODO por ahora imprime cientos de habitantes nada mas
                break;                                  //faltaria hacer el segundo map reduce

            default:
                throw new RuntimeException("Bad query parameter");
        }

        System.exit(0);
    }

    private static void query1printer(Map<String, Integer> map){
        String[] keys  = {"0-14", "15-64", "65-?"};
        for (String key :keys)
            System.out.println(key + " = " + map.getOrDefault(key,0));      //mostramos para todos los intervalos
    }
    private static void query2printer(Map<String, Double> map){
        String[] keys  = {"0","1","2","3","4","5","6","7","8","9"};
        for (String key :keys) {
            if(map.containsKey(key))                                // si no esta no lo mostramos
                System.out.println(key + " = " + String.format("%.02f", map.get(key)));     //dos decimales
        }
    }

    private static void query3printer(List<Map.Entry<String,Double>> list){
        for (Map.Entry<String,Double> entry :list) {
            System.out.println(entry.getKey() + " = " + String.format("%.02f", entry.getValue()));     //dos decimales
        }
    }

    private static void query4printer(List<Map.Entry<String,Integer>> list){
        for (Map.Entry<String,Integer> entry :list) {
            System.out.println(entry.getKey() + " = " + entry.getValue());
        }
    }

    private static void query5printer(Map<String[],Integer> map){
        for (Map.Entry<String[],Integer> entry : map.entrySet()) {
            System.out.println(String.format("%s(%s) = ", entry.getKey()[0], entry.getKey()[1]) + entry.getValue());
        }
    }


}
