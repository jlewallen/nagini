package com.page5of4.nagini;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.SerializerFactory;

import com.page5of4.nagini.serializers.GsonSerializer;
import com.page5of4.nagini.serializers.JacksonSerializer;
import com.page5of4.nagini.serializers.UUIDSerializer;

public class CustomizableSerializerFactory implements SerializerFactory {
   private static final String GSON_SERIALIER_TYPE_NAME = "gson";
   private static final String JACKSON_SERIALIER_TYPE_NAME = "jackson";
   private static final String UUID_SERIALIER_TYPE_NAME = "uuid";

   private final SerializerFactory chainedFactory;
   private final Map<String, Class<? extends Serializer<?>>> map = new HashMap<String, Class<? extends Serializer<?>>>();

   public CustomizableSerializerFactory(SerializerFactory chainedFactory) {
      super();
      this.add(UUID_SERIALIER_TYPE_NAME, UUIDSerializer.class);
      this.add(GSON_SERIALIER_TYPE_NAME, GsonSerializer.class);
      this.add(JACKSON_SERIALIER_TYPE_NAME, JacksonSerializer.class);
      this.chainedFactory = chainedFactory;
   }

   public void add(String name, Class<? extends Serializer<?>> klass) {
      map.put(name, klass);
   }

   @Override
   public Serializer<?> getSerializer(SerializerDefinition serializerDef) {
      String name = serializerDef.getName();
      if(!map.containsKey(name)) {
         return chainedFactory.getSerializer(serializerDef);
      }
      try {
         Class<?> klass = map.get(name);
         try {
            if(serializerDef.hasSchemaInfo()) {
               Constructor<?> ctor = klass.getConstructor(String.class);
               return (Serializer<?>)ctor.newInstance(serializerDef.getCurrentSchemaInfo());
            }
            else {
               Constructor<?> ctor = klass.getConstructor();
               return (Serializer<?>)ctor.newInstance();
            }
         }
         catch(Throwable e) {
            throw new RuntimeException("Error creating Serializer: " + klass.getName(), e);
         }
      }
      catch(Exception e) {
         throw new RuntimeException(e);
      }
   }
}
