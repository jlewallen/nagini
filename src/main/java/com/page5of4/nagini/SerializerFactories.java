package com.page5of4.nagini;

import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.SerializerFactory;

public class SerializerFactories {
   public static SerializerFactory defaultChain() {
      return new SerializerSchemaMappingFactory(new CustomizableSerializerFactory(new DefaultSerializerFactory()));
   }
}
