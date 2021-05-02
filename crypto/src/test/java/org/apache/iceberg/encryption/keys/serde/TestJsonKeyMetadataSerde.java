package org.apache.iceberg.encryption.keys.serde;

import org.apache.iceberg.encryption.serde.JsonKeyMetadataSerde;
import org.apache.iceberg.encryption.serde.JsonReadableKeyMetadata;
import org.apache.iceberg.encryption.serde.JsonWritableKeyMetadata;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestJsonKeyMetadataSerde {
  private static final String EXPECTED_JSON =
      "{\"myField\":\"blah\",\"provider\":\"myId\",\"iv\":\"YWI=\"}";
  private static final String MY_KEY_PROVIDER_ID = "myId";
  private static final byte[] MY_IV = {'a', 'b'};
  private static final String MY_FIELD = "myField";
  private static final String MY_FIELD_VALUE = "blah";

  JsonKeyMetadataSerde uut = JsonKeyMetadataSerde.INSTANCE;
  String jsonString;
  Map<String, String> map;

  @Test
  public void testWrite() {
    JsonWritableKeyMetadata keyMetadata =
        uut.getWritableKeyMetadata()
            .withIv(MY_IV)
            .withKeyProviderId(MY_KEY_PROVIDER_ID)
            .setField(MY_FIELD, MY_FIELD_VALUE);
    String myString = new String(keyMetadata.getBytes(), StandardCharsets.UTF_8);
    assertEquals(EXPECTED_JSON, myString);
  }

  @Test
  public void testRead() {
    String output = EXPECTED_JSON;
    byte[] bytes = output.getBytes(StandardCharsets.UTF_8);
    ByteBuffer bb = ByteBuffer.wrap(bytes);
    JsonReadableKeyMetadata keyMetadata = uut.toReadableKeyMetadata(bb);
    assertEquals(keyMetadata.get(MY_FIELD), MY_FIELD_VALUE);
    assertNull(keyMetadata.get("other_field"));
    assertEquals(keyMetadata.getKeyProviderId(), MY_KEY_PROVIDER_ID);
    assertArrayEquals(keyMetadata.getIv(), MY_IV);
  }

  @Test(expected = IllegalStateException.class)
  public void testNoIvWrite() {
    JsonWritableKeyMetadata keyMetadata =
        uut.getWritableKeyMetadata().withKeyProviderId(MY_KEY_PROVIDER_ID);
    keyMetadata.getBytes();
  }

  @Test(expected = IllegalStateException.class)
  public void testNoKeyProviderId() {
    JsonWritableKeyMetadata keyMetadata = uut.getWritableKeyMetadata().withIv(MY_IV);
    keyMetadata.getBytes();
  }
}
