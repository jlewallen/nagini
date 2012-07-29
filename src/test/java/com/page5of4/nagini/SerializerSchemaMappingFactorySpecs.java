package com.page5of4.nagini;

import static org.fest.assertions.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.IdentitySerializer;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.json.JsonTypeSerializer;

public class SerializerSchemaMappingFactorySpecs {
   private SerializerSchemaMappingFactory factory;

   @Before
   public void before() {
      factory = new SerializerSchemaMappingFactory(new DefaultSerializerFactory());
   }

   @Test
   public void when_creating_serializer_with_no_schema_info() {
      Serializer<?> serializer = factory.getSerializer(new SerializerDefinition("identity"));
      assertThat(serializer).isInstanceOfAny(IdentitySerializer.class);
   }

   @Test
   public void when_creating_serializer_with_schema_info_and_no_mapping() {
      Serializer<?> serializer = factory.getSerializer(new SerializerDefinition("json", "[ { 'id': 'string' } ]"));
      assertThat(serializer).isInstanceOfAny(JsonTypeSerializer.class);
   }

   @Test
   public void when_creating_serializer_with_schema_info_and_a_mapping() {
      factory.addSchemaMapping("user-name", "[ { 'id': 'string', 'name': 'string' } ]");
      factory.addSchemaMapping("user-id", "[ { 'id': 'string' } ]");
      Serializer<?> serializer = factory.getSerializer(new SerializerDefinition("json", "user-name"));
      assertThat(serializer).isInstanceOfAny(JsonTypeSerializer.class);
   }
}
