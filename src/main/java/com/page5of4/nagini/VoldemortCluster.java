package com.page5of4.nagini;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import voldemort.cluster.Cluster;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.StoreDefinition;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;

import com.google.common.collect.Lists;
import com.page5of4.nagini.VoldemortClusterBuilder.Helpers;

public class VoldemortCluster {
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