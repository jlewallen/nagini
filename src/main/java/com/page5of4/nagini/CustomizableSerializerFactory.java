package com.page5of4.nagini;

import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.SerializerFactory;

import com.page5of4.nagini.serializers.GsonSerializer;
import com.page5of4.nagini.serializers.JacksonSerializer;
import com.page5of4.nagini.serializers.UUIDSerializer;

public class CustomizableSerializerFactory implements SerializerFactory {
   private final DefaultSerializerFactory defaultSerializerFactory = new DefaultSerializerFactory();

   @Override
   public Serializer<?> getSerializer(SerializerDefinition serializerDef) {
      String name = serializerDef.getName();
      if(name.equals("uuid")) {
         return new UUIDSerializer();
      }
      else if(name.equals("jackson")) {
         return new JacksonSerializer(serializerDef.getCurrentSchemaInfo());
      }
      else if(name.equals("gson")) {
         return new GsonSerializer(serializerDef.getCurrentSchemaInfo());
      }
      return defaultSerializerFactory.getSerializer(serializerDef);
   }
}
