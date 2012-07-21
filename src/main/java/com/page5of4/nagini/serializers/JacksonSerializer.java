package com.page5of4.nagini.serializers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import org.codehaus.jackson.map.ObjectMapper;

import voldemort.serialization.Serializer;

public class JacksonSerializer implements Serializer<Object> {
   private final ObjectMapper mapper = new ObjectMapper();
   private final Class<?> schema;

   public JacksonSerializer(String schema) {
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
      try {
         ByteArrayOutputStream stream = new ByteArrayOutputStream();
         mapper.writeValue(stream, object);
         return stream.toByteArray();
      }
      catch(Exception e) {
         throw new RuntimeException(e);
      }
   }

   @Override
   public Object toObject(byte[] bytes) {
      try {
         ByteArrayInputStream stream = new ByteArrayInputStream(bytes);
         return mapper.readValue(stream, schema);
      }
      catch(Exception e) {
         throw new RuntimeException(e);
      }
   }
}
