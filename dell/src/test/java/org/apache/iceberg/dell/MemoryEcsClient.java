/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.dell;

import com.amazonaws.util.Md5Utils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.dell.impl.ContentAndETagImpl;
import org.apache.iceberg.dell.impl.ObjectHeadInfoImpl;
import org.apache.iceberg.dell.impl.ObjectKeysImpl;

public class MemoryEcsClient implements EcsClient {

  private final ObjectBaseKey baseKey;

  private final ConcurrentMap<ObjectKey, EcsObject> data = new ConcurrentHashMap<>();

  public static EcsClient create(Map<String, String> properties) {
    return new MemoryEcsClient(EcsCatalogProperties.getObjectBaseKey(properties));
  }

  public MemoryEcsClient(ObjectBaseKey baseKey) {
    this.baseKey = baseKey;
  }

  @Override
  public ObjectKeys getKeys() {
    return new ObjectKeysImpl(baseKey);
  }

  @Override
  public Map<String, String> getProperties() {
    return null;
  }

  @Override
  public Optional<ObjectHeadInfo> head(ObjectKey key) {
    return Optional.ofNullable(data.get(key)).map(EcsObject::getHeadInfo);
  }

  @Override
  public InputStream inputStream(ObjectKey key, long pos) {
    EcsObject object = data.get(key);
    byte[] content = object.getContent();
    return new ByteArrayInputStream(content, (int) pos, content.length);
  }

  @Override
  public OutputStream outputStream(ObjectKey key) {
    return new WrappedOutputStream(key);
  }

  @Override
  public ContentAndETag readAll(ObjectKey key) {
    return data.get(key).getContentAndETag();
  }

  @Override
  public boolean replace(ObjectKey key, String eTag, byte[] bytes, Map<String, String> userMetadata) {
    EcsObject original = data.get(key);
    if (original == null) {
      return false;
    }
    if (!original.getHeadInfo().getETag().equals(eTag)) {
      return false;
    }
    return data.replace(key, original, EcsObject.create(bytes, userMetadata));
  }

  @Override
  public boolean writeIfAbsent(ObjectKey key, byte[] bytes, Map<String, String> userMetadata) {
    return data.putIfAbsent(key, EcsObject.create(bytes, userMetadata)) == null;
  }

  @Override
  public boolean copyObjectIfAbsent(ObjectKey fromKey, String eTag, ObjectKey toKey) {
    EcsObject original = data.get(fromKey);
    if (original == null) {
      return false;
    }
    if (!original.getHeadInfo().getETag().equals(eTag)) {
      return false;
    }
    return data.putIfAbsent(toKey, original) == null;
  }

  @Override
  public void deleteObject(ObjectKey key) {
    data.remove(key);
  }

  @Override
  public <T> List<T> listDelimiterAll(ObjectKey prefix, Function<ObjectKey, Optional<T>> filterAndMapper) {
    String delimiter = getKeys().getDelimiter();
    String prefixKey;
    if (prefix.getKey().isEmpty()) {
      prefixKey = "";
    } else if (prefix.getKey().endsWith(delimiter)) {
      prefixKey = prefix.getKey();
    } else {
      prefixKey = prefix.getKey() + delimiter;
    }
    int prefixLength = prefixKey.length();
    return data.keySet().stream()
        .filter(key -> {
          if (!Objects.equals(key.getBucket(), prefix.getBucket())) {
            return false;
          }
          if (!key.getKey().startsWith(prefixKey)) {
            return false;
          }
          return key.getKey().indexOf(delimiter, prefixLength) < 0;
        })
        .sorted(Comparator.comparing(ObjectKey::getBucket).thenComparing(ObjectKey::getKey))
        .flatMap(key -> filterAndMapper.apply(key).map(Stream::of).orElse(Stream.empty()))
        .collect(Collectors.toList());
  }

  @Override
  public EcsClient copy() {
    return this;
  }

  @Override
  public void close() {
  }

  public static class EcsObject {

    private final ObjectHeadInfo headInfo;
    private final byte[] content;

    public EcsObject(ObjectHeadInfo headInfo, byte[] content) {
      this.headInfo = headInfo;
      this.content = content;
    }

    public ObjectHeadInfo getHeadInfo() {
      return headInfo;
    }

    public byte[] getContent() {
      return Arrays.copyOf(content, content.length);
    }

    public ContentAndETag getContentAndETag() {
      return new ContentAndETagImpl(getHeadInfo(), getContent());
    }

    public static EcsObject create(byte[] content, Map<String, String> userMetadata) {
      return new EcsObject(
          new ObjectHeadInfoImpl(content.length, Md5Utils.md5AsBase64(content), userMetadata),
          content);
    }
  }

  public class WrappedOutputStream extends OutputStream {

    private final ObjectKey key;
    private final ByteArrayOutputStream byteArrayOutput = new ByteArrayOutputStream();

    public WrappedOutputStream(ObjectKey key) {
      this.key = key;
    }

    @Override
    public void write(int b) {
      byteArrayOutput.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) {
      byteArrayOutput.write(b, off, len);
    }

    @Override
    public void close() {
      data.put(key, EcsObject.create(byteArrayOutput.toByteArray(), Collections.emptyMap()));
    }
  }
}
