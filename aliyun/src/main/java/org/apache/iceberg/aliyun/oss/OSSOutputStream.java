/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.aliyun.oss;

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.AbortMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadResult;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PartETag;
import com.aliyun.oss.model.PutObjectRequest;
import com.aliyun.oss.model.UploadPartRequest;
import com.aliyun.oss.model.UploadPartResult;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;
import org.apache.iceberg.aliyun.AliyunProperties;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Predicates;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.io.CountingOutputStream;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class OSSOutputStream extends PositionOutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(OSSOutputStream.class);

  private static volatile ExecutorService executorService;

  private final StackTraceElement[] createStack;
  private final OSS client;
  private final OSSURI uri;
  private final AliyunProperties aliyunProperties;

  // For multipart uploading.
  private final long multiPartSize;
  private final long multiPartThresholdSize;
  private final File stagingDirectory;
  private final List<File> stagingFiles = Lists.newArrayList();
  private final Map<File, CompletableFuture<UploadPartResult>> multiPartMap = Maps.newHashMap();
  private String multipartUploadId = null;
  private File currentStagingFile;
  private CountingOutputStream stream;

  private long pos = 0;
  private boolean closed = false;

  OSSOutputStream(OSS client, OSSURI uri, AliyunProperties aliyunProperties) throws IOException {
    this.client = client;
    this.uri = uri;
    this.aliyunProperties = aliyunProperties;

    this.createStack = Thread.currentThread().getStackTrace();

    this.multiPartSize = aliyunProperties.ossMultiPartSize();
    this.multiPartThresholdSize = aliyunProperties.ossMultipartThresholdSize();
    this.stagingDirectory = new File(aliyunProperties.ossStagingDirectory());

    // Initialize the executor service lazily.
    initializeExecutorService();

    newStream();
  }

  private void initializeExecutorService() {
    if (executorService == null) {
      synchronized (OSSOutputStream.class) {
        if (executorService == null) {
          executorService = MoreExecutors.getExitingExecutorService(
              (ThreadPoolExecutor) Executors.newFixedThreadPool(
                  aliyunProperties.ossMultipartUploadThreads(),
                  new ThreadFactoryBuilder()
                      .setDaemon(true)
                      .setNameFormat("iceberg-oss-file-io-upload-%d")
                      .build()));
        }
      }
    }
  }

  @Override
  public long getPos() {
    return pos;
  }

  @Override
  public void flush() throws IOException {
    Preconditions.checkState(!closed, "Already closed.");

    stream.flush();
  }

  @Override
  public void write(int b) throws IOException {
    Preconditions.checkState(!closed, "Already closed.");

    if (stream.getCount() >= multiPartSize) {
      newStream();
      uploadParts();
    }

    stream.write(b);
    pos += 1;

    // switch to multipart upload
    if (multipartUploadId == null && pos >= multiPartThresholdSize) {
      initializeMultiPartUpload();
      uploadParts();
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    Preconditions.checkState(!closed, "Already closed.");

    int remaining = len;
    int relativeOffset = off;

    // Write the remainder of the part size to the staging file
    // and continue to write new staging files if the write is
    // larger than the part size.
    while (stream.getCount() + remaining > multiPartSize) {
      int writeSize = (int) (multiPartSize - stream.getCount());

      stream.write(b, relativeOffset, writeSize);
      remaining -= writeSize;
      relativeOffset += writeSize;

      newStream();
      uploadParts();
    }

    stream.write(b, relativeOffset, remaining);
    pos += len;

    // switch to multipart upload
    if (multipartUploadId == null && pos >= multiPartThresholdSize) {
      initializeMultiPartUpload();
      uploadParts();
    }
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }

    super.close();
    closed = true;

    try {
      stream.close();
      completeUploads();
    } finally {
      cleanUpStagingFiles();
    }
  }

  private void newStream() throws IOException {
    if (stream != null) {
      stream.close();
    }

    currentStagingFile = File.createTempFile("oss-file-io-", ".tmp", stagingDirectory);
    currentStagingFile.deleteOnExit();
    stagingFiles.add(currentStagingFile);

    stream = new CountingOutputStream(new BufferedOutputStream(new FileOutputStream(currentStagingFile)));
  }

  private void initializeMultiPartUpload() {
    InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(uri.bucket(), uri.key());
    InitiateMultipartUploadResult result = client.initiateMultipartUpload(request);
    multipartUploadId = result.getUploadId();
  }

  private void uploadParts() {
    // exit if multipart has not been initiated
    if (multipartUploadId == null) {
      return;
    }

    stagingFiles.stream()
        // do not upload the file currently being written
        .filter(f -> closed || !f.equals(currentStagingFile))
        // do not upload any files that have already been processed
        .filter(Predicates.not(multiPartMap::containsKey))
        .forEach(f -> {
          UploadPartRequest uploadRequest = new UploadPartRequest(uri.bucket(), uri.key(),
              multipartUploadId, stagingFiles.indexOf(f) + 1, uncheckedInputStream(f), f.length());

          CompletableFuture<UploadPartResult> future = CompletableFuture.supplyAsync(
              () -> client.uploadPart(uploadRequest),
              executorService
          ).whenComplete((result, thrown) -> {
            try {
              Files.deleteIfExists(f.toPath());
            } catch (IOException e) {
              LOG.warn("Failed to delete staging file: {}", f, e);
            }

            if (thrown != null) {
              LOG.error("Failed to upload part: {}", f, thrown);
              abortUpload();
            }
          });

          multiPartMap.put(f, future);
        });
  }

  private void abortUpload() {
    if (multipartUploadId != null) {
      try {
        AbortMultipartUploadRequest request = new AbortMultipartUploadRequest(uri.bucket(), uri.key(),
            multipartUploadId);
        client.abortMultipartUpload(request);
      } finally {
        cleanUpStagingFiles();
      }
    }
  }

  private void cleanUpStagingFiles() {
    Tasks.foreach(stagingFiles)
        .suppressFailureWhenFinished()
        .onFailure((file, thrown) -> LOG.warn("Failed to delete staging file: {}", file, thrown))
        .run(File::delete);
  }

  private void completeUploads() {
    if (multipartUploadId == null) {
      long contentLength = stagingFiles.stream().mapToLong(File::length).sum();
      LOG.debug("Uploading {} staging files to oss, total byte size is: {}", stagingFiles.size(), contentLength);

      InputStream contentStream = new BufferedInputStream(stagingFiles.stream()
          .map(OSSOutputStream::uncheckedInputStream)
          .reduce(SequenceInputStream::new)
          .orElseGet(() -> new ByteArrayInputStream(new byte[0])));

      ObjectMetadata metadata = new ObjectMetadata();
      metadata.setContentLength(contentLength);

      PutObjectRequest request = new PutObjectRequest(uri.bucket(), uri.key(), contentStream, metadata);
      client.putObject(request);
    } else {
      uploadParts();
      completeMultiPartUpload();
    }
  }

  private void completeMultiPartUpload() {
    Preconditions.checkState(closed, "Complete upload called on open stream: " + uri);

    List<PartETag> completedPartETags =
        multiPartMap.values()
            .stream()
            .map(CompletableFuture::join)
            .sorted(Comparator.comparing(UploadPartResult::getPartNumber))
            .map(UploadPartResult::getPartETag)
            .collect(Collectors.toList());

    CompleteMultipartUploadRequest request = new CompleteMultipartUploadRequest(uri.bucket(), uri.key(),
        multipartUploadId, completedPartETags);

    Tasks.foreach(request)
        .noRetry()
        .onFailure((r, thrown) -> {
          LOG.error("Failed to complete multipart upload request: {}", r, thrown);
          abortUpload();
        })
        .throwFailureWhenFinished()
        .run(client::completeMultipartUpload);
  }

  private static InputStream uncheckedInputStream(File file) {
    try {
      return new FileInputStream(file);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @SuppressWarnings("checkstyle:NoFinalizer")
  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    if (!closed) {
      close(); // releasing resources is more important than printing the warning.
      String trace = Joiner.on("\n\t").join(Arrays.copyOfRange(createStack, 1, createStack.length));
      LOG.warn("Unclosed output stream created by:\n\t{}", trace);
    }
  }
}
