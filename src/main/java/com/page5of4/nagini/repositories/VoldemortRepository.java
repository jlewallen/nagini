package com.page5of4.nagini.repositories;

import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;

public class VoldemortRepository<K, V> implements Repository<K, V> {
   private final StoreClient<K, V> client;

   public VoldemortRepository(StoreClient<K, V> client) {
      super();
      this.client = client;
   }

   public VoldemortRepository(StoreClientFactory storeClientFactory, String storeName) {
      this(storeClientFactory.<K, V> getStoreClient(storeName));
   }

   @Override
   public V get(K key) {
      return client.getValue(key);
   }

   @Override
   public void delete(K key) {
      client.delete(key);
   }

   @Override
   public void add(K key, V value) {
      client.put(key, value);
   }
}
