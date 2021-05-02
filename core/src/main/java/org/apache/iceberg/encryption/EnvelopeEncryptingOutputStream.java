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

package org.apache.iceberg.encryption;

import java.io.IOException;
import org.apache.iceberg.io.PositionOutputStream;

/**
 * A encrypting output stream based on the given envelope encryption metadata.
 * TODO: currently just pass through, will add concrete logic in a separated PR.
 */
public class EnvelopeEncryptingOutputStream extends PositionOutputStream {

  private final PositionOutputStream rawOutput;
  private final EnvelopeMetadata metadata;

  public EnvelopeEncryptingOutputStream(PositionOutputStream rawOutput, EnvelopeMetadata metadata) {
    this.rawOutput = rawOutput;
    this.metadata = metadata;
  }

  @Override
  public long getPos() throws IOException {
    return rawOutput.getPos();
  }

  @Override
  public void write(int b) throws IOException {
    rawOutput.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    rawOutput.write(b, off, len);
  }
}
