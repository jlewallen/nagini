package com.page5of4.nagini;

import java.util.Map;

import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.SerializerFactory;

import com.google.common.collect.Maps;

public class SerializerSchemaMappingFactory implements SerializerFactory {
   private final Map<String, String> map = Maps.newConcurrentMap();
   private final SerializerFactory serializerFactory;

   public SerializerSchemaMappingFactory(SerializerFactory serializerFactory) {
      super();
      this.serializerFactory = serializerFactory;
   }

   public void addSchemaMapping(String key, String value) {
      map.put(key, value);
   }

   public void addSchemaMapping(String key, Class<?> value) {
      addSchemaMapping(key, value.getName());
   }

   public String findSchemaFor(String schema) {
      if(map.containsKey(schema)) {
         return map.get(schema);
      }
      return schema;
   }

   @Override
   public Serializer<?> getSerializer(SerializerDefinition serializerDef) {
      Map<Integer, String> byVersion = serializerDef.getAllSchemaInfoVersions();
      Map<Integer, String> mappedSchemaInfo = Maps.newHashMap();
      for(Integer version : byVersion.keySet()) {
         String schema = findSchemaFor(byVersion.get(version));
         mappedSchemaInfo.put(version, schema);
      }
      SerializerDefinition mappedDefinition = new SerializerDefinition(serializerDef.getName(),
            mappedSchemaInfo,
            serializerDef.hasVersion(),
            serializerDef.getCompression());
      return serializerFactory.getSerializer(mappedDefinition);
   }
}
