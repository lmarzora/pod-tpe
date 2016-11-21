package ar.itba.edu.pod.hazel.client;

import ar.itba.edu.pod.hazel.Tuple;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;

/**
 * Created by lumarzo on 21/11/16.
 */
public class MapLoader {

    private static final String MAP_NAME = "52066-54449-";
    private static Logger logger = LoggerFactory.getLogger(TPEClient.class);

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
        String inputPath = System.getProperty("inPath");
        String mapName = MAP_NAME + inputPath;
        final IMap<Integer, Tuple> map = client.getMap(mapName);

        int lineStart = Integer.valueOf(System.getProperty("lineStart"));
        int lineEnd = Integer.valueOf(System.getProperty("lineEnd"));

        long time;
        final InputStream is = new FileInputStream(inputPath);//TPEClient.class.getClassLoader().getResourceAsStream(inputPath);
        final LineNumberReader reader = new LineNumberReader(new InputStreamReader(is));
        System.out.println("Loading Map");
        logger.info("Inicio de la lectura del archivo");
        time = System.currentTimeMillis();
        if (reader.getLineNumber() == 0)
            reader.readLine();
        String line;
        reader.setLineNumber(lineStart);
        while (reader.getLineNumber() < lineEnd && (line = reader.readLine()) != null) {
            map.put(reader.getLineNumber(), TupleParser.parse(line));
            logger.debug("Copiada linea " + reader.getLineNumber());
        }
        time = System.currentTimeMillis() - time;
        logger.info("Fin lectura del archivo");
        logger.info("Tiempo total de carga del mapa en ms = " + time);
        System.out.println("Map loaded");

        System.out.println("map size = " + map.size());

        System.exit(1);
    }
}