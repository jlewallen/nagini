package com.page5of4.nagini.serializers;

import static org.junit.Assert.assertEquals;

import java.util.UUID;

import org.junit.Test;

public class UUIDSerializerSpecs {
   @Test
   public void when_converting_to_bytes_and_back() {
      UUIDSerializer serializer = new UUIDSerializer();
      UUID id = UUID.randomUUID();
      byte[] bytes = serializer.toBytes(id);
      assertEquals(serializer.toObject(bytes), id);
   }
}
