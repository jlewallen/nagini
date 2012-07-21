package com.page5of4.nagini.serializers;

import voldemort.serialization.Serializer;

import com.google.gson.Gson;

public class GsonSerializer implements Serializer<Object> {
   private final Gson gson = new Gson();
   private final Class<?> schema;

   public GsonSerializer(String schema) {
      super();
      try {
         ClassLoader loader = getClass().getClassLoader();
         if(Thread.currentThread().getContextClassLoader() != null) {
            loader = Thread.currentThread().getContextClassLoader();
         }
         this.schema = loader.loadClass(schema);
      }
      catch(ClassNotFoundException e) {
         throw new RuntimeException(e);
      }
   }

   @Override
   public byte[] toBytes(Object object) {
      return gson.toJson(object).getBytes();
   }

   @Override
   public Object toObject(byte[] bytes) {
      return gson.fromJson(new String(bytes), schema);
   }
}
