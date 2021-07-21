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

package org.apache.iceberg.aliyun.oss.mock;

import com.aliyun.oss.OSSErrorCode;
import com.aliyun.oss.model.Bucket;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;
import static org.springframework.http.HttpStatus.OK;
import static org.springframework.http.HttpStatus.PARTIAL_CONTENT;
import static org.springframework.http.HttpStatus.REQUESTED_RANGE_NOT_SATISFIABLE;

@RestController
public class LocalOSSController {
  private static final Logger LOG = LoggerFactory.getLogger(LocalOSSController.class);

  @Autowired
  private LocalStore localStore;

  @RequestMapping(value = "/{bucketName}", method = RequestMethod.PUT, produces = "application/xml")
  public void putBucket(@PathVariable String bucketName) throws IOException {
    if (localStore.getBucket(bucketName) != null) {
      throw new OssException(409, OSSErrorCode.BUCKET_ALREADY_EXISTS, bucketName + " already exists.");
    }

    localStore.createBucket(bucketName);
  }

  @RequestMapping(value = "/{bucketName}", method = RequestMethod.DELETE, produces = "application/xml")
  public void deleteBucket(@PathVariable String bucketName) throws IOException {
    verifyBucketExistence(bucketName);

    localStore.deleteBucket(bucketName);
  }

  @RequestMapping(value = "/{bucketName:.+}/**", method = RequestMethod.PUT)
  public ResponseEntity<String> putObject(@PathVariable String bucketName, HttpServletRequest request) {
    verifyBucketExistence(bucketName);
    String filename = filenameFrom(bucketName, request);
    try (ServletInputStream inputStream = request.getInputStream()) {
      ObjectMetadata metadata = localStore.putObject(bucketName,
          filename,
          inputStream,
          request.getContentType(),
          request.getHeader(HttpHeaders.CONTENT_ENCODING),
          ImmutableMap.of());

      HttpHeaders responseHeaders = new HttpHeaders();
      responseHeaders.setETag("\"" + metadata.getContentMD5() + "\"");
      responseHeaders.setLastModified(metadata.getLastModificationDate());

      return new ResponseEntity<>(responseHeaders, OK);
    } catch (Exception e) {
      LOG.error("Failed to put object - bucket: {} - object: {}", bucketName, filename, e);
      return new ResponseEntity<>(e.getMessage(), INTERNAL_SERVER_ERROR);
    }
  }

  @RequestMapping(value = "/{bucketName:.+}/**", method = RequestMethod.DELETE)
  public void deleteObject(@PathVariable String bucketName, HttpServletRequest request) {
    verifyBucketExistence(bucketName);

    localStore.deleteObject(bucketName, filenameFrom(bucketName, request));
  }

  @RequestMapping(value = "/{bucketName:.+}/**", method = RequestMethod.HEAD)
  public ResponseEntity<String> getObjectMeta(@PathVariable String bucketName, HttpServletRequest request) {
    verifyBucketExistence(bucketName);
    ObjectMetadata metadata = verifyObjectExistence(bucketName, filenameFrom(bucketName, request));

    HttpHeaders headers = new HttpHeaders();
    headers.setETag("\"" + metadata.getContentMD5() + "\"");
    headers.setLastModified(metadata.getLastModificationDate());
    headers.setContentLength(metadata.getContentLength());

    return new ResponseEntity<>(headers, OK);
  }

  @SuppressWarnings("checkstyle:AnnotationUseStyle")
  @RequestMapping(
      value = "/{bucketName:.+}/**",
      method = RequestMethod.GET,
      produces = "application/xml")
  public void getObject(@PathVariable String bucketName,
                        @RequestHeader(value = "Range", required = false) Range range,
                        HttpServletRequest request,
                        HttpServletResponse response) throws IOException {
    verifyBucketExistence(bucketName);

    String filename = filenameFrom(bucketName, request);
    ObjectMetadata metadata = verifyObjectExistence(bucketName, filename);

    if (range != null) {
      long fileSize = metadata.getContentLength();
      long bytesToRead = Math.min(fileSize - 1, range.end()) - range.start() + 1;

      if (bytesToRead < 0 || fileSize < range.start()) {
        response.setStatus(REQUESTED_RANGE_NOT_SATISFIABLE.value());
        response.flushBuffer();
        return;
      }

      response.setStatus(PARTIAL_CONTENT.value());
      response.setHeader(HttpHeaders.ACCEPT_RANGES, "bytes");
      response.setHeader(HttpHeaders.CONTENT_RANGE, String.format("bytes %s-%s/%s",
          range.start(), bytesToRead + range.start() + 1, metadata.getContentLength()));
      response.setHeader(HttpHeaders.ETAG, "\"" + metadata.getContentMD5() + "\"");
      response.setDateHeader(HttpHeaders.LAST_MODIFIED, metadata.getLastModificationDate());
      response.setContentType(metadata.getContentType());
      response.setContentLengthLong(bytesToRead);

      try (OutputStream outputStream = response.getOutputStream()) {
        try (FileInputStream fis = new FileInputStream(metadata.getDataFile())) {
          fis.skip(range.start());
          IOUtils.copy(new BoundedInputStream(fis, bytesToRead), outputStream);
        }
      }

    } else {
      response.setHeader(HttpHeaders.ACCEPT_RANGES, "bytes");
      response.setHeader(HttpHeaders.ETAG, "\"" + metadata.getContentMD5() + "\"");
      response.setDateHeader(HttpHeaders.LAST_MODIFIED, metadata.getLastModificationDate());
      response.setContentType(metadata.getContentType());
      response.setContentLengthLong(metadata.getContentLength());

      try (OutputStream outputStream = response.getOutputStream()) {
        try (FileInputStream fis = new FileInputStream(metadata.getDataFile())) {
          IOUtils.copy(fis, outputStream);
        }
      }
    }
  }

  @RequestMapping(
      value = "/{bucketName:.+}/**",
      params = "uploads",
      method = RequestMethod.POST,
      produces = "application/xml")
  public InitiateMultipartUploadResult initializeMultiPartUpload(@PathVariable String bucketName,
                                                                 HttpServletRequest request) {
    verifyBucketExistence(bucketName);

    String filename = filenameFrom(bucketName, request);
    String uploadId = UUID.randomUUID().toString();

    localStore.prepareMultipartUpload(bucketName, filename, uploadId);
    return new InitiateMultipartUploadResult(bucketName, filename, uploadId);
  }

  @RequestMapping(
      value = "/{bucketName:.+}/**",
      params = {"uploadId", "partNumber"},
      method = RequestMethod.PUT)
  public ResponseEntity<String> uploadPart(@PathVariable String bucketName,
                                           @RequestParam String uploadId,
                                           @RequestParam String partNumber,
                                           HttpServletRequest request)
      throws IOException {
    verifyBucketExistence(bucketName);
    verifyPartNumberLimits(partNumber);

    String etag = localStore.putPart(bucketName, uploadId, partNumber, request.getInputStream());

    HttpHeaders responseHeaders = new HttpHeaders();
    responseHeaders.setETag(String.format("\"%s\"", etag));

    return new ResponseEntity<>(responseHeaders, OK);
  }

  @SuppressWarnings("checkstyle:AnnotationUseStyle")
  @RequestMapping(
      value = "/{bucketName:.+}/**",
      params = {"uploadId"},
      method = RequestMethod.POST,
      produces = "application/xml")
  public ResponseEntity<CompleteMultipartUploadResult> completeMultiPartUpload(@PathVariable String bucketName,
                                                                               @RequestParam String uploadId,
                                                                               @RequestBody
                                                                                   CompleteMultipartUploadRequest
                                                                                   requestBody,
                                                                               HttpServletRequest request)
      throws IOException {
    verifyBucketExistence(bucketName);

    String filename = filenameFrom(bucketName, request);
    verifyMultiParts(requestBody.parts);

    String eTag = localStore.completeMultipartUpload(bucketName, filename, uploadId, requestBody.parts);
    return new ResponseEntity<>(
        new CompleteMultipartUploadResult(request.getRequestURL().toString(), bucketName, filename, eTag),
        new HttpHeaders(), OK
    );
  }

  @SuppressWarnings("checkstyle:AnnotationUseStyle")
  @RequestMapping(
      value = "/{bucketName:.+}/**",
      params = {"uploadId"},
      method = RequestMethod.DELETE,
      produces = "application/xml")
  public void abortMultipartUploads(@PathVariable String bucketName,
                                    @RequestParam String uploadId,
                                    HttpServletRequest request) {
    verifyBucketExistence(bucketName);

    String filename = filenameFrom(bucketName, request);
    localStore.abortMultipartUpload(bucketName, filename, uploadId);
  }

  private void verifyBucketExistence(String bucketName) {
    Bucket bucket = localStore.getBucket(bucketName);
    if (bucket == null) {
      throw new OssException(404, OSSErrorCode.NO_SUCH_BUCKET, "The specified bucket does not exist. ");
    }
  }

  private ObjectMetadata verifyObjectExistence(String bucketName, String filename) {
    ObjectMetadata objectMetadata = null;
    try {
      objectMetadata = localStore.getObjectMetadata(bucketName, filename);
    } catch (IOException e) {
      LOG.error("Failed to get the object metadata, bucket: {}, object: {}.", bucketName, filename, e);
    }

    if (objectMetadata == null) {
      throw new OssException(404, OSSErrorCode.NO_SUCH_KEY, "The specify oss key does not exists.");
    }

    return objectMetadata;
  }

  private static String filenameFrom(@PathVariable String bucketName, HttpServletRequest request) {
    String requestUri = request.getRequestURI();
    return requestUri.substring(requestUri.indexOf(bucketName) + bucketName.length() + 1);
  }

  private void verifyPartNumberLimits(String partNumberString) {
    boolean isValid;
    try {
      int partNumber = Integer.parseInt(partNumberString);
      isValid = partNumber >= 1 && partNumber <= 10000;
    } catch (NumberFormatException e) {
      isValid = false;
    }

    if (!isValid) {
      throw new OssException(HttpStatus.BAD_REQUEST.value(), "InvalidArgument",
          "Part number must be an integer between 1 and 10000, inclusive");
    }
  }

  private void verifyMultiParts(List<Part> parts) {
    int prevPartNumber = 0;
    for (Part part : parts) {
      if (part.partNumber < prevPartNumber) {
        throw new OssException(HttpStatus.BAD_REQUEST.value(), "InvalidPartOrder",
            "The list of parts was not in ascending order. The parts list must be specified in " +
                "order by part number.");
      }
    }
  }

  @ControllerAdvice
  public static class OSSMockExceptionHandler extends ResponseEntityExceptionHandler {

    @ExceptionHandler
    public ResponseEntity<ErrorResponse> handleOSSException(OssException ex) {
      LOG.info("Responding with status {} - {}, {}", ex.status, ex.code, ex.message);

      ErrorResponse errorResponse = new ErrorResponse();
      errorResponse.setCode(ex.getCode());
      errorResponse.setMessage(ex.getMessage());

      HttpHeaders headers = new HttpHeaders();
      headers.setContentType(MediaType.APPLICATION_XML);

      return ResponseEntity.status(ex.status)
          .headers(headers)
          .body(errorResponse);
    }
  }

  public static class OssException extends RuntimeException {

    private final int status;
    private final String code;
    private final String message;

    public OssException(final int status, final String code, final String message) {
      super(message);
      this.status = status;
      this.code = code;
      this.message = message;
    }

    public String getCode() {
      return code;
    }

    @Override
    public String getMessage() {
      return message;
    }
  }

  @JsonRootName("Error")
  public static class ErrorResponse {
    @JsonProperty("Code")
    private String code;

    @JsonProperty("Message")
    private String message;

    public void setCode(String code) {
      this.code = code;
    }

    public void setMessage(String message) {
      this.message = message;
    }
  }

  @JsonRootName("InitiateMultipartUploadResult")
  public static class InitiateMultipartUploadResult {

    @JsonProperty("Bucket")
    private final String bucketName;

    @JsonProperty("Key")
    private final String fileName;

    @JsonProperty("UploadId")
    private final String uploadId;

    public InitiateMultipartUploadResult(String bucketName,
                                         String fileName,
                                         String uploadId) {
      this.bucketName = bucketName;
      this.fileName = fileName;
      this.uploadId = uploadId;
    }
  }

  @JsonRootName("Part")
  public static class Part {

    @JsonProperty("PartNumber")
    Integer partNumber;

    @JsonProperty("LastModified")
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    Date lastModified;

    @JsonProperty("ETag")
    String etag;

    @JsonProperty("Size")
    Long size;
  }

  @JsonRootName("CompleteMultipartUpload")
  public static class CompleteMultipartUploadRequest {

    @JsonProperty("Part")
    @JacksonXmlElementWrapper(useWrapping = false)
    private List<Part> parts;
  }

  @JsonRootName("CompleteMultipartUploadResult")
  public class CompleteMultipartUploadResult {

    @JsonProperty("Location")
    private final String location;

    @JsonProperty("Bucket")
    private final String bucket;

    @JsonProperty("Key")
    private final String key;

    @JsonProperty("ETag")
    private final String etag;

    public CompleteMultipartUploadResult(String location, String bucket, String key, String etag) {
      this.location = location;
      this.bucket = bucket;
      this.key = key;
      this.etag = etag;
    }
  }
}
