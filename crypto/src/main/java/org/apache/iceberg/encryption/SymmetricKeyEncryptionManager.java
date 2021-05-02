package org.apache.iceberg.encryption;

import org.apache.iceberg.encryption.crypto.CryptoFormat;
import org.apache.iceberg.encryption.crypto.SymmetricKeyDecryptedInputFile;
import org.apache.iceberg.encryption.crypto.SymmetricKeyEncryptedOutputFile;
import org.apache.iceberg.encryption.keys.*;
import org.apache.iceberg.encryption.serde.KeyMetadataSerde;
import org.apache.iceberg.encryption.serde.ReadableKeyMetadata;
import org.apache.iceberg.encryption.serde.WritableKeyMetadata;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

/**
 * A {@link org.apache.iceberg.encryption.SymmetricKeyEncryptionManager} is the top level {@link EncryptionManager} used to
 * encrypt and decrypt each {@link org.apache.iceberg.DataFile}.
 *
 * <p>On encrypting a new file:
 *
 * <ul>
 *   <li>Call the {@link KeyProviderDelegator} to create a new {@link Key} using the default {@link
 *       KeyProvider} for new {@link Key} generation. It will return the key and retrieval info in
 *       order to retrieve that key.
 *   <li>Generate an IV with a length supported by the {@link CryptoFormat}.
 *   <li>Create a new {@link WritableKeyMetadata} and tell the {@link KeyProviderDelegator} to set
 *       its retrieval info. Create an {@link SymmetricKeyEncryptedOutputFile} using the new {@link
 *       Key} and iv.
 *   <li>Return the new file along with the output of the {@link WritableKeyMetadata} in a {@link
 *       BasicEncryptedOutputFile}
 * </ul>
 *
 * WritableKeyMetadata} in a {@link BasicEncryptedOutputFile}
 *
 * <p>On decrypting an existing file:
 *
 * <ul>
 *   <li>Use the {@link KeyMetadataSerde} to create a {@link ReadableKeyMetadata}
 *   <li>Call the {@link KeyProviderDelegator} to read the {@link Key} from the {@link
 *       ReadableKeyMetadata} given the provider
 *   <li>Construct a {@link SymmetricKeyDecryptedInputFile} from the {@link Key}.
 * </ul>
 */
public class SymmetricKeyEncryptionManager implements EncryptionManager {
  // TODO IV is always 0 for now. Create iv generator to generate iv with length.
  private static final byte[] EMPTY_IV = new byte[0];

  private final KeyProviderDelegator keyProviderDelegator;
  private final CryptoFormat cryptoFormat;
  private final KeyMetadataSerde<?, ?> serde;

  /**
   * @param keyProviderDelegator Delegates key creation and key retrieval to individual {@link
   *     KeyProvider}
   * @param cryptoFormat Used during decryption and is opinionated about dekLength and iv length.
   * @param serde Used to read and write {@link KeyMetadata}
   */
  public SymmetricKeyEncryptionManager(
      KeyProviderDelegator keyProviderDelegator,
      CryptoFormat cryptoFormat,
      KeyMetadataSerde<?, ?> serde) {
    this.keyProviderDelegator = keyProviderDelegator;
    this.cryptoFormat = cryptoFormat;
    this.serde = serde;
  }

  @Override
  public InputFile decrypt(EncryptedInputFile encrypted) {
    EncryptionKeyMetadata encryptionKeyMetadata = encrypted.keyMetadata(); // TODO check null
    ReadableKeyMetadata reader = serde.toReadableKeyMetadata(encryptionKeyMetadata.buffer());
    Key key = keyProviderDelegator.decrypt(reader);
    return new SymmetricKeyDecryptedInputFile(
        encrypted.encryptedInputFile(), cryptoFormat, key.plaintext(), reader.getIv());
  }

  @Override
  public EncryptedOutputFile encrypt(OutputFile plaintextOutputFile) {
    KeyAndKeyRetrievalInfo<?> keyAndKeyRetrievalInfo =
        keyProviderDelegator.createKey(cryptoFormat.dekLength());
    return encryptHelper(plaintextOutputFile, keyAndKeyRetrievalInfo);
  }

  /** Helper method to make types work. */
  private <M extends KeyMaterials> EncryptedOutputFile encryptHelper(
      OutputFile plaintextOutputFile, KeyAndKeyRetrievalInfo<M> keyAndKeyRetrievalInfo) {

    WritableKeyMetadata writer =
        keyProviderDelegator
            .setKeyRetrievalInfo(keyAndKeyRetrievalInfo, serde.getWritableKeyMetadata())
            .withIv(EMPTY_IV);

    OutputFile encryptingOutputFile =
        new SymmetricKeyEncryptedOutputFile(
            plaintextOutputFile, cryptoFormat, keyAndKeyRetrievalInfo.key().plaintext(), EMPTY_IV);

    return new BasicEncryptedOutputFile(
        encryptingOutputFile, writer.toBasicEncryptionKeyMetadata());
  }
}
