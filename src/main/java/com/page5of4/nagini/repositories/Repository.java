package com.page5of4.nagini.repositories;

public interface Repository<K, V> {
   V get(K key);

   void delete(K key);

   void add(K key, V value);
}
