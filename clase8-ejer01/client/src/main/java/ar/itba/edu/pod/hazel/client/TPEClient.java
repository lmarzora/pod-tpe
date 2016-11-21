package ar.itba.edu.pod.hazel.client;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class TPEClient {
    private static final String MAP_NAME = "52066-54449-";
    private static final String MAP_NAME_2 = MAP_NAME + "aux";
    private static Logger logger = LoggerFactory.getLogger(TPEClient.class);
    private static PrintWriter printer;

    // el directorio wc dentro en un directorio llamado "resources"
    // al mismo nivel que la carpeta src, etc.

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
        System.out.println(String.format("input = %s", System.getProperty("inPath")));
        System.out.println(String.format("output = %s", System.getProperty("outPath")));
        String inputPath = System.getProperty("inPath");
        printer = new PrintWriter(System.getProperty("outPath"));
        String[] filename = inputPath.split("/");
        String nameOfMap  = filename[filename.length-1].split(".")[0];
        String mapName = MAP_NAME + nameOfMap;
        final IMap<Integer, Tuple> map = client.getMap(mapName);

        long time;
        final InputStream is = new FileInputStream(inputPath);//TPEClient.class.getClassLoader().getResourceAsStream(inputPath);
        final LineNumberReader reader = new LineNumberReader(new InputStreamReader(is));
        if(map.size() < reader.lines().count()-1) {
            System.out.println("Loading Map");
            logger.info("Inicio de la lectura del archivo");
            time = System.currentTimeMillis();

            reader.readLine();
            String line;
            while ((line = reader.readLine()) != null) {
                map.put(reader.getLineNumber(), TupleParser.parse(line));
                logger.debug("Copiada linea " + reader.getLineNumber());
            }
            time = System.currentTimeMillis() - time;
            logger.info("Fin lectura del archivo");
            logger.info("Tiempo total de carga del mapa en ms = " + time);
            System.out.println("Map loaded");
        } else{
            System.out.println("Map Already loaded");
        }

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
                logger.info("Inicio del trabajo map/reduce");
                time = System.currentTimeMillis();
                final ICompletableFuture<Map<String, Integer>> future1 = job
                        .mapper(new Query1Mapper())
                        .combiner(new CountCombiner())
                        .reducer(new CountReducer())
                        .submit();

                // Tomar resultado e Imprimirlo
                final Map<String, Integer> ans1 = future1.get();
                time = System.currentTimeMillis() - time;
                logger.info("Fin del trabajo map/reduce");
                logger.info("Tiempo total de carga de la  query " + queryNumber + " = " + time);
                query1printer(ans1);
                break;

            case "2":
                logger.info("Inicio del trabajo map/reduce");
                time = System.currentTimeMillis();
                final ICompletableFuture<List<Map.Entry<String, Double>>> future2 = job
                        .mapper(new Query2Mapper())
                        .reducer(new Query2Reducer())
                        .submit(new KeyDescendantCollator<>());

                // Tomar resultado e Imprimirlo
                final List<Map.Entry<String, Double>> ans2 = future2.get();
                time = System.currentTimeMillis() - time;
                logger.info("Fin del trabajo map/reduce");
                logger.info("Tiempo total de carga de la  query " + queryNumber + " = " + time);
                query2printer(ans2);
                break;

            case "3":
                int n = Integer.valueOf(System.getProperty("n","5"));
                logger.info("Inicio del trabajo map/reduce");
                time = System.currentTimeMillis();
                final ICompletableFuture<List<Map.Entry<String,Double>>> future3 = job
                        .mapper(new Query3Mapper())
                        .combiner(new AverageCombiner())
                        .reducer(new AverageReducer())
                        .submit(new TopNCollator(n));

                // Tomar resultado e Imprimirlo
                final List<Map.Entry<String,Double>> ans3 = future3.get();
                time = System.currentTimeMillis() - time;
                logger.info("Fin del trabajo map/reduce");
                logger.info("Tiempo total de carga de la  query " + queryNumber + " = " + time);
                query3printer(ans3);
                break;

            case "4":

                String prov = System.getProperty("prov","Buenos Aires");
                int tope = Integer.valueOf(System.getProperty("tope","500"));
                logger.info("Inicio del trabajo map/reduce");
                time = System.currentTimeMillis();
                final ICompletableFuture<List<Map.Entry<String,Integer>>> future4 = job
                        .mapper(new Query4Mapper(prov))
                        .combiner(new CountCombiner())
                        .reducer(new CountReducer())
                        .submit(new BelowLimitCollator(tope));

                // Tomar resultado e Imprimirlo
                final List<Map.Entry<String,Integer>> ans4 = future4.get();
                time = System.currentTimeMillis() - time;
                logger.info("Fin del trabajo map/reduce");
                logger.info("Tiempo total de carga de la  query " + queryNumber + " = " + time);
                query4printer(ans4);
                break;

            case "5":
                logger.info("Inicio del trabajo map/reduce");
                time = System.currentTimeMillis();
                final ICompletableFuture<Map<String, Integer>> future5_1 = job
                        .mapper(new Query5Mapper())
                        .combiner(new CountCombiner())
                        .reducer(new CountReducer())
                        .submit();

                // Tomar resultado como base para segundo map reduce
                final Map<String,Integer> habitantesPorDepto = future5_1.get();

                final IMap<String, Integer> map2 = client.getMap(MAP_NAME_2);
                map2.putAll(habitantesPorDepto);

                final KeyValueSource<String, Integer> source2 = KeyValueSource.fromMap(map2);

                final Job<String, Integer> secondJob = tracker.newJob(source2);

                final ICompletableFuture<List<Map.Entry<Integer,List<String>>>> future5_2 = secondJob
                        .mapper(new SwapperMapper())
                        .reducer(new Query5Reducer())
                        .submit(new Query5OrderCollator());

                final List<Map.Entry<Integer,List<String>>> ans5 = future5_2.get();
                time = System.currentTimeMillis() - time;
                logger.info("Fin del trabajo map/reduce");
                logger.info("Tiempo total de carga de la  query " + queryNumber + " = " + time);
                query5printer(ans5);

                break;

            default:
                throw new RuntimeException("Bad query parameter");
        }

        printer.close();
        System.exit(0);
    }

    private static void query1printer(Map<String, Integer> map){
        String[] keys  = {"0-14", "15-64", "65-?"};
        for (String key :keys)
            printer.println(key + " = " + map.getOrDefault(key,0));      //mostramos para todos los intervalos
    }
    private static void query2printer(List<Map.Entry<String, Double>> list){
        for (Map.Entry<String, Double> entry : list) {
            printer.println(String.format("%s = %.02f", entry.getKey(), entry.getValue()));     //dos decimales
        }
    }

//    private static void query2printer(Map<String, Double> map){
//        String[] keys  = {"0","1","2","3","4","5","6","7","8","9"};
//        for (String key :keys) {
//            if(map.containsKey(key))                                // si no esta no lo mostramos
//                printer.println(key + " = " + String.format("%.02f", map.get(key)));     //dos decimales
//        }
//    }

    private static void query3printer(List<Map.Entry<String,Double>> list){
        for (Map.Entry<String,Double> entry :list) {
            printer.println(entry.getKey() + " = " + String.format("%.02f", entry.getValue()));     //dos decimales
        }
    }

    private static void query4printer(List<Map.Entry<String,Integer>> list){
        for (Map.Entry<String,Integer> entry :list) {
            printer.println(entry.getKey() + " = " + entry.getValue());
        }
    }

    private static void query5printer(List<Map.Entry<Integer,List<String>>> list){
        for (Map.Entry<Integer, List<String>> entry : list) {
            printer.println(entry.getKey());
            entry.getValue().forEach(printer::println);
        }
    }


}
