package com.page5of4.nagini;

import java.io.IOException;
import java.util.Date;
import java.util.UUID;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.serialization.SerializerDefinition;
import voldemort.versioning.Versioned;


public class VoldemortSpecs {
   private static final Logger logger = LoggerFactory.getLogger(VoldemortSpecs.class);

   @Test
   public void test() throws IOException {
      VoldemortCluster cluster = VoldemortClusterBuilder.make().
            numberOfNodes(2).
            withStore("junk", new SerializerDefinition("uuid"), new SerializerDefinition("gson", User.class.getName())).
            start();

      User user = new User(UUID.randomUUID(), "Jacob", "Lewallen", new Date());

      String bootstrapUrl = cluster.getBootstrapUrl();
      StoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setSerializerFactory(new CustomizableSerializerFactory()).setBootstrapUrls(bootstrapUrl));
      StoreClient<UUID, User> client = factory.getStoreClient("junk");

      for(short i = 0; i < 10; ++i) {
         Versioned<User> version = client.get(user.getId(), new Versioned<User>(null));
         logger.info("Got: " + version.getValue());
         version.setObject(user);
         client.put(user.getId(), version);
      }

      cluster.stop();

      logger.info("Done");
   }

   public static class User {
      private final UUID id;
      private final String firstName;
      private final String lastName;
      private final Date registeredAt;

      public UUID getId() {
         return id;
      }

      public String getFirstName() {
         return firstName;
      }

      public String getLastName() {
         return lastName;
      }

      public Date getRegisteredAt() {
         return registeredAt;
      }

      public User(UUID id, String firstName, String lastName, Date registeredAt) {
         super();
         this.id = id;
         this.firstName = firstName;
         this.lastName = lastName;
         this.registeredAt = registeredAt;
      }
   }
}
