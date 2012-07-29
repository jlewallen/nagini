package com.page5of4.nagini;

import java.util.Map;

import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.SerializerFactory;

public class SerializerFactories {
   public static SerializerFactory defaultChain() {
      return builder().build();
   }

   public static SerializerFactoryChainBuilder builder() {
      return new SerializerFactoryChainBuilder();
   }

   public static class SerializerFactoryChainBuilder {
      private final SerializerSchemaMappingFactory mappingFactory;
      private final SerializerFactory rootFactory;

      public SerializerFactoryChainBuilder() {
         super();
         rootFactory = mappingFactory = new SerializerSchemaMappingFactory(new CustomizableSerializerFactory(new DefaultSerializerFactory()));
      }

      public SerializerFactoryChainBuilder mapSchema(String key, String to) {
         mappingFactory.addSchemaMapping(key, to);
         return this;
      }

      public SerializerFactoryChainBuilder mapSchema(String key, Class<?> to) {
         mappingFactory.addSchemaMapping(key, to);
         return this;
      }

      public SerializerFactoryChainBuilder mapSchema(Map<String, String> mapping) {
         mappingFactory.addSchemaMappings(mapping);
         return this;
      }

      public SerializerFactory build() {
         return rootFactory;
      }
   }
}
