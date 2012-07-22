package com.page5of4.nagini;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import voldemort.client.RoutingTier;
import voldemort.client.protocol.RequestFormatType;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.SerializerDefinition;
import voldemort.server.RequestRoutingType;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.UnreachableStoreException;
import voldemort.store.memory.InMemoryStorageConfiguration;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.slop.strategy.HintedHandoffStrategyType;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.ByteArray;
import voldemort.utils.Props;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class VoldemortClusterBuilder {
   private static final Logger logger = LoggerFactory.getLogger(VoldemortClusterBuilder.class);
   private final File home;
   private final List<StoreDefinition> stores = new ArrayList<StoreDefinition>();
   private Cluster cluster;

   public VoldemortClusterBuilder() {
      super();
      this.home = new File("voldemort-clusters/test");
      this.home.mkdirs();
   }

   public static VoldemortClusterBuilder make() {
      return new VoldemortClusterBuilder();
   }

   public VoldemortClusterBuilder numberOfNodes(int nodes) {
      cluster = Helpers.getLocalCluster(nodes, 4);
      return this;
   }

   public VoldemortClusterBuilder with(StoreDefinition storeDefinition) {
      stores.add(storeDefinition);
      return this;
   }

   public VoldemortClusterBuilder withJsonBackedStore(String name) {
      return with(createStoreDefinition("json", name, 2, 1, 1, 2, 2, RoutingStrategyType.CONSISTENT_STRATEGY));
   }

   public VoldemortClusterBuilder withStringBackedStore(String name) {
      return with(createStoreDefinition("string", name, 2, 1, 1, 2, 2, RoutingStrategyType.CONSISTENT_STRATEGY));
   }

   public VoldemortClusterBuilder withStore(String name, String keySerializer, String valueSerializer) {
      return withStore(name, new SerializerDefinition(keySerializer), new SerializerDefinition(valueSerializer));
   }

   public VoldemortClusterBuilder withStore(String name, SerializerDefinition keySerializer, SerializerDefinition valueSerializer) {
      return with(new StoreDefinitionBuilder().setName(name)
            .setType(InMemoryStorageConfiguration.TYPE_NAME)
            .setKeySerializer(keySerializer)
            .setValueSerializer(valueSerializer)
            .setRoutingPolicy(RoutingTier.SERVER)
            .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
            .setReplicationFactor(2)
            .setPreferredReads(1)
            .setRequiredReads(1)
            .setPreferredWrites(2)
            .setRequiredWrites(2)
            .setHintedHandoffStrategy(HintedHandoffStrategyType.ANY_STRATEGY)
            .build());
   }

   private StoreDefinition createStoreDefinition(String serializer, String storeName, int replicationFactor, int preads, int rreads, int pwrites, int rwrites, String strategyType) {
      SerializerDefinition serDef = new SerializerDefinition(serializer);
      return new StoreDefinitionBuilder().setName(storeName)
            .setType(InMemoryStorageConfiguration.TYPE_NAME)
            .setKeySerializer(serDef)
            .setValueSerializer(serDef)
            .setRoutingPolicy(RoutingTier.SERVER)
            .setRoutingStrategyType(strategyType)
            .setReplicationFactor(replicationFactor)
            .setPreferredReads(preads)
            .setRequiredReads(rreads)
            .setPreferredWrites(pwrites)
            .setRequiredWrites(rwrites)
            .setHintedHandoffStrategy(HintedHandoffStrategyType.ANY_STRATEGY)
            .build();
   }

   public VoldemortCluster start() {
      return new VoldemortCluster(home, cluster, stores).start();
   }

   public static class VoldemortCluster {
      private static final Logger logger = LoggerFactory.getLogger(VoldemortCluster.class);
      private final SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2, 10000, 100000, 32 * 1024);
      private final Cluster cluster;
      private final List<StoreDefinition> stores;
      private final List<VoldemortServer> servers = Lists.newArrayList();
      private final File home;

      public VoldemortCluster(File home, Cluster cluster, List<StoreDefinition> stores) {
         super();
         this.home = home;
         this.cluster = cluster;
         this.stores = stores;
      }

      public VoldemortCluster start() {
         try {
            for(int i = 0; i < cluster.getNumberOfNodes(); ++i) {
               VoldemortConfig config = Helpers.createServerConfigWithDefs(true, i, home.getAbsolutePath(), cluster, stores, new Properties());
               new File(config.getMetadataDirectory()).mkdir();
               VoldemortServer server = new VoldemortServer(config);
               server.start();
               Helpers.waitForServerStart(socketStoreFactory, server.getIdentityNode());
               servers.add(server);
            }
            return this;
         }
         catch(IOException e) {
            throw new RuntimeException(e);
         }
      }

      public void stop() {
         for(VoldemortServer server : servers) {
            try {
               server.stop();
            }
            catch(Exception e) {
               logger.error("Error stopping Server", e);
            }
         }
      }

      public String getBootstrapUrl() {
         return String.format("tcp://127.0.0.1:%d", cluster.getNodeById(0).getSocketPort());
      }
   }

   public static class Helpers {
      private static final Logger logger = LoggerFactory.getLogger(VoldemortClusterBuilder.class);

      public static int[] findFreePorts(int n) {
         int[] ports = new int[n];
         ServerSocket[] sockets = new ServerSocket[n];
         try {
            for(int i = 0; i < n; i++) {
               sockets[i] = new ServerSocket(0);
               ports[i] = sockets[i].getLocalPort();
            }
            return ports;
         }
         catch(IOException e) {
            throw new RuntimeException(e);
         }
         finally {
            for(int i = 0; i < n; i++) {
               try {
                  if(sockets[i] != null) sockets[i].close();
               }
               catch(IOException e) {
               }
            }
         }
      }

      public static Cluster getLocalCluster(int numberOfNodes) {
         return getLocalCluster(numberOfNodes, findFreePorts(3 * numberOfNodes), null);
      }

      public static Cluster getLocalCluster(int numberOfNodes, int[][] partitionMap) {
         return getLocalCluster(numberOfNodes, findFreePorts(3 * numberOfNodes), partitionMap);
      }

      public static Cluster getLocalCluster(int numberOfNodes, int[] ports, int[][] partitionMap) {
         if(3 * numberOfNodes != ports.length) throw new IllegalArgumentException(3 * numberOfNodes + " ports required but only " + ports.length + " given.");
         List<Node> nodes = new ArrayList<Node>();
         for(int i = 0; i < numberOfNodes; i++) {
            List<Integer> partitions = ImmutableList.of(i);
            if(null != partitionMap) {
               partitions = new ArrayList<Integer>(partitionMap[i].length);
               for(int p : partitionMap[i]) {
                  partitions.add(p);
               }
            }

            nodes.add(new Node(i, "127.0.0.1", ports[3 * i], ports[3 * i + 1], ports[3 * i + 2], partitions));
            logger.info(String.format("Cluster: Node(%d) Partitions(%s)", i, StringUtils.join(partitions, " ")));
         }

         return new Cluster("test-cluster", nodes);
      }

      public static Cluster getLocalCluster(int numberOfNodes, int partitionsPerNode) {
         int[] ports = findFreePorts(3 * numberOfNodes);

         List<Integer> ids = new ArrayList<Integer>();
         for(int i = 0; i < numberOfNodes * partitionsPerNode; ++i) {
            ids.add(i);
         }
         Collections.shuffle(ids, new Random(92873498274L));

         List<Node> nodes = new ArrayList<Node>();
         for(int i = 0; i < numberOfNodes; ++i) {
            List<Integer> partitions = new ArrayList<Integer>();
            for(int j = 0; j < partitionsPerNode; ++j) {
               partitions.add(ids.get(i * partitionsPerNode + j));
            }
            Collections.sort(partitions);
            nodes.add(new Node(i, "127.0.0.1", ports[3 * i], ports[3 * i + 1], ports[3 * i + 2], partitions));
            logger.info(String.format("Cluster: Node(%d) Partitions(%s)", i, StringUtils.join(partitions, " ")));
         }
         return new Cluster("test-cluster", nodes);
      }

      public static void waitForServerStart(SocketStoreFactory socketStoreFactory, Node node) {
         boolean success = false;
         int retries = 10;
         Store<ByteArray, ?, ?> store = null;
         while(retries-- > 0) {
            store = getSocketStore(socketStoreFactory, MetadataStore.METADATA_STORE_NAME, node.getSocketPort());
            try {
               store.get(new ByteArray(MetadataStore.CLUSTER_KEY.getBytes()), null);
               success = true;
            }
            catch(UnreachableStoreException e) {
               store.close();
               store = null;
               System.out.println("UnreachableSocketStore sleeping will try again " + retries + " times.");
               try {
                  Thread.sleep(1000);
               }
               catch(InterruptedException e1) {
                  // ignore
               }
            }
         }

         store.close();
         if(!success) throw new RuntimeException("Failed to connect with server:" + node);
      }

      public static Store<ByteArray, byte[], byte[]> getSocketStore(SocketStoreFactory storeFactory, String storeName, int port) {
         return getSocketStore(storeFactory, storeName, port, RequestFormatType.VOLDEMORT_V1);
      }

      public static Store<ByteArray, byte[], byte[]> getSocketStore(SocketStoreFactory storeFactory, String storeName, int port, RequestFormatType type) {
         return getSocketStore(storeFactory, storeName, "localhost", port, type);
      }

      public static Store<ByteArray, byte[], byte[]> getSocketStore(SocketStoreFactory storeFactory, String storeName, String host, int port, RequestFormatType type) {
         return getSocketStore(storeFactory, storeName, host, port, type, false);
      }

      public static Store<ByteArray, byte[], byte[]> getSocketStore(SocketStoreFactory storeFactory, String storeName, String host, int port, RequestFormatType type, boolean isRouted) {
         RequestRoutingType requestRoutingType = RequestRoutingType.getRequestRoutingType(isRouted, false);
         return storeFactory.create(storeName, host, port, type, requestRoutingType);
      }

      public static VoldemortConfig createServerConfigWithDefs(boolean useNio, int nodeId, String baseDir, Cluster cluster, List<StoreDefinition> stores, Properties properties) throws IOException {
         File clusterXml = new File(baseDir, "cluster.xml");
         File storesXml = new File(baseDir, "stores.xml");

         if(cluster != null) {
            ClusterMapper clusterMapper = new ClusterMapper();
            FileWriter writer = new FileWriter(clusterXml);
            writer.write(clusterMapper.writeCluster(cluster));
            writer.close();
         }

         if(stores != null) {
            StoreDefinitionsMapper storeDefMapper = new StoreDefinitionsMapper();
            FileWriter writer = new FileWriter(storesXml);
            writer.write(storeDefMapper.writeStoreList(stores));
            writer.close();
         }

         return createServerConfig(useNio, nodeId, baseDir, clusterXml.getAbsolutePath(), storesXml.getAbsolutePath(), properties);

      }

      public static VoldemortConfig createServerConfig(boolean useNio, int nodeId, String baseDir, String clusterFile, String storeFile, Properties properties) throws IOException {
         Props props = new Props();
         props.put("node.id", nodeId);
         props.put("voldemort.home", baseDir + "/node-" + nodeId);
         props.put("bdb.cache.size", 1 * 1024 * 1024);
         props.put("bdb.write.transactions", "true");
         props.put("bdb.flush.transactions", "true");
         props.put("jmx.enable", "false");
         props.put("enable.mysql.engine", "true");
         props.loadProperties(properties);

         VoldemortConfig config = new VoldemortConfig(props);
         config.setMysqlDatabaseName("voldemort");
         config.setMysqlUsername("voldemort");
         config.setMysqlPassword("voldemort");
         config.setStreamMaxReadBytesPerSec(10 * 1000 * 1000);
         config.setStreamMaxWriteBytesPerSec(10 * 1000 * 1000);
         config.setUseNioConnector(useNio);

         for(String path : new String[] { config.getMetadataDirectory(), config.getDataDirectory() }) {
            File tempDir = new File(path);
            tempDir.mkdirs();
            tempDir.deleteOnExit();
         }

         // copy cluster.xml / stores.xml to temp metadata dir.
         if(null != clusterFile) FileUtils.copyFile(new File(clusterFile), new File(config.getMetadataDirectory() + File.separatorChar + "cluster.xml"));
         if(null != storeFile) FileUtils.copyFile(new File(storeFile), new File(config.getMetadataDirectory() + File.separatorChar + "stores.xml"));

         return config;
      }
   }
}
