package com.page5of4.nagini.serializers;

import java.util.UUID;

import voldemort.serialization.Serializer;

public class UUIDSerializer implements Serializer<UUID> {
   @Override
   public byte[] toBytes(UUID object) {
      long msb = object.getMostSignificantBits();
      long lsb = object.getLeastSignificantBits();
      byte[] buffer = new byte[16];

      for(int i = 0; i < 8; i++) {
         buffer[i] = (byte)(msb >>> 8 * (7 - i));
      }
      for(int i = 8; i < 16; i++) {
         buffer[i] = (byte)(lsb >>> 8 * (7 - i));
      }
      return buffer;
   }

   @Override
   public UUID toObject(byte[] bytes) {
      long msb = 0;
      long lsb = 0;
      for(int i = 0; i < 8; i++)
         msb = (msb << 8) | (bytes[i] & 0xff);
      for(int i = 8; i < 16; i++)
         lsb = (lsb << 8) | (bytes[i] & 0xff);
      return new UUID(msb, lsb);
   }
}
