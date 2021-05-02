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

package org.apache.iceberg.encryption;

import java.util.function.Supplier;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.PropertyUtil;

/**
 * Encryption manager that performs envelope encryption.
 * <p>
 *
 */
public class EnvelopeEncryptionManager implements EncryptionManager {

  private final EnvelopeEncryptionSpec spec;
  private final EnvelopeKeyManager keyManager;
  private final boolean pushdown;

  public EnvelopeEncryptionManager(
      TableMetadata tableMetadata,
      EnvelopeEncryptionSpec spec,
      EnvelopeKeyManager keyManager) {
    this.spec = spec;
    this.keyManager = keyManager;
    this.pushdown = PropertyUtil.propertyAsBoolean(tableMetadata.properties(),
        TableProperties.ENCRYPTION_PUSHDOWN_ENABLED, TableProperties.ENCRYPTION_PUSHDOWN_ENABLED_DEFAULT);
  }

  @Override
  public InputFile decrypt(EncryptedInputFile encrypted) {
    if (pushdown) {
      return encrypted.encryptedInputFile();
    }

    EnvelopeMetadata metadata = EnvelopeMetadataParser.fromJson(encrypted.keyMetadata().buffer());
    keyManager.decrypt(metadata);
    return new EnvelopeEncryptedInputFile(encrypted.encryptedInputFile(), metadata);
  }

  @Override
  public EncryptedOutputFile encrypt(OutputFile rawOutput) {
    return encryptWithConfig(rawOutput, spec::dataFileConfig);
  }

  @Override
  public EncryptedOutputFile encryptManifestFile(OutputFile rawOutput) {
    return encryptWithConfig(rawOutput, spec::manifestFileConfig);
  }

  @Override
  public EncryptedOutputFile encryptManifestList(OutputFile rawOutput) {
    return encryptWithConfig(rawOutput, spec::manifestListConfig);
  }

  private EncryptedOutputFile encryptWithConfig(OutputFile rawOutput, Supplier<EnvelopeConfig> configSupplier) {
    EnvelopeMetadata metadata = keyManager.generate(configSupplier.get());
    if (pushdown) {
      return EncryptedFiles.encryptedOutput(rawOutput, metadata);
    }

    return EncryptedFiles.encryptedOutput(new EnvelopeEncryptedOutputFile(rawOutput, metadata), metadata);
  }
}
