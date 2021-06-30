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

package org.apache.iceberg.arrow.vectorized.parquet;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.apache.parquet.io.ParquetDecodingException;

/**
 * A values reader for Parquet's run-length encoded data that reads column data in batches instead of one value at a
 * time. This is based off of the VectorizedRleValuesReader class in Apache Spark with these changes:
 * <p>
 * Writes batches of values retrieved to Arrow vectors. If all pages of a column within the row group are not dictionary
 * encoded, then dictionary ids are eagerly decoded into actual values before writing them to the Arrow vectors
 * </p>
 */
@SuppressWarnings("checkstyle:VisibilityModifier")
public class BaseVectorizedParquetValuesReader extends ValuesReader {
  // Current decoding mode. The encoded data contains groups of either run length encoded data
  // (RLE) or bit packed data. Each group contains a header that indicates which group it is and
  // the number of values in the group.
  enum Mode {
    RLE,
    PACKED
  }

  // Encoded data.
  private ByteBufferInputStream inputStream;

  // bit/byte width of decoded data and utility to batch unpack them.
  private int bitWidth;
  private int bytesWidth;
  private BytePacker packer;

  // Current decoding mode and values
  Mode mode;
  int currentCount;
  int currentValue;

  // Buffer of decoded values if the values are PACKED.
  int[] packedValuesBuffer = new int[16];
  int packedValuesBufferIdx = 0;

  // If true, the bit width is fixed. This decoder is used in different places and this also
  // controls if we need to read the bitwidth from the beginning of the data stream.
  private final boolean fixedWidth;
  private final boolean readLength;
  final int maxDefLevel;

  final boolean setArrowValidityVector;

  public BaseVectorizedParquetValuesReader(int maxDefLevel, boolean setValidityVector) {
    this.maxDefLevel = maxDefLevel;
    this.fixedWidth = false;
    this.readLength = false;
    this.setArrowValidityVector = setValidityVector;
  }

  public BaseVectorizedParquetValuesReader(
      int bitWidth,
      int maxDefLevel,
      boolean setValidityVector) {
    this.fixedWidth = true;
    this.readLength = bitWidth != 0;
    this.maxDefLevel = maxDefLevel;
    this.setArrowValidityVector = setValidityVector;
    init(bitWidth);
  }

  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream in) throws IOException {
    this.inputStream = in;
    if (fixedWidth) {
      // initialize for repetition and definition levels
      if (readLength) {
        int length = readIntLittleEndian();
        this.inputStream = in.sliceStream(length);
      }
    } else {
      // initialize for values
      if (in.available() > 0) {
        init(in.read());
      }
    }
    if (bitWidth == 0) {
      // 0 bit width, treat this as an RLE run of valueCount number of 0's.
      this.mode = Mode.RLE;
      this.currentCount = valueCount;
      this.currentValue = 0;
    } else {
      this.currentCount = 0;
    }
  }

  /**
   * Initializes the internal state for decoding ints of `bitWidth`.
   */
  private void init(int bw) {
    Preconditions.checkArgument(bw >= 0 && bw <= 32, "bitWidth must be >= 0 and <= 32");
    this.bitWidth = bw;
    this.bytesWidth = BytesUtils.paddedByteCountFromBits(bw);
    this.packer = Packer.LITTLE_ENDIAN.newBytePacker(bw);
  }

  /**
   * Reads the next varint encoded int.
   */
  private int readUnsignedVarInt() throws IOException {
    int value = 0;
    int shift = 0;
    int byteRead;
    do {
      byteRead = inputStream.read();
      value |= (byteRead & 0x7F) << shift;
      shift += 7;
    } while ((byteRead & 0x80) != 0);
    return value;
  }

  /**
   * Reads the next 4 byte little endian int.
   */
  private int readIntLittleEndian() throws IOException {
    int ch4 = inputStream.read();
    int ch3 = inputStream.read();
    int ch2 = inputStream.read();
    int ch1 = inputStream.read();
    return (ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0);
  }

  /**
   * Reads the next byteWidth little endian int.
   */
  private int readIntLittleEndianPaddedOnBitWidth() throws IOException {
    switch (bytesWidth) {
      case 0:
        return 0;
      case 1:
        return inputStream.read();
      case 2: {
        int ch2 = inputStream.read();
        int ch1 = inputStream.read();
        return (ch1 << 8) + ch2;
      }
      case 3: {
        int ch3 = inputStream.read();
        int ch2 = inputStream.read();
        int ch1 = inputStream.read();
        return (ch1 << 16) + (ch2 << 8) + (ch3 << 0);
      }
      case 4: {
        return readIntLittleEndian();
      }
    }
    throw new RuntimeException("Non-supported bytesWidth: " + bytesWidth);
  }

  /**
   * Reads the next group.
   */
  void readNextGroup() {
    try {
      int header = readUnsignedVarInt();
      this.mode = (header & 1) == 0 ? Mode.RLE : Mode.PACKED;
      switch (mode) {
        case RLE:
          this.currentCount = header >>> 1;
          this.currentValue = readIntLittleEndianPaddedOnBitWidth();
          return;
        case PACKED:
          int numGroups = header >>> 1;
          this.currentCount = numGroups * 8;
          if (this.packedValuesBuffer.length < this.currentCount) {
            this.packedValuesBuffer = new int[this.currentCount];
          }
          packedValuesBufferIdx = 0;
          int valueIndex = 0;
          while (valueIndex < this.currentCount) {
            // values are bit packed 8 at a time, so reading bitWidth will always work
            ByteBuffer buffer = inputStream.slice(bitWidth);
            this.packer.unpack8Values(buffer, buffer.position(), this.packedValuesBuffer, valueIndex);
            valueIndex += 8;
          }
          return;
        default:
          throw new ParquetDecodingException("not a valid mode " + this.mode);
      }
    } catch (IOException e) {
      throw new ParquetDecodingException("Failed to read from input stream", e);
    }
  }

  @Override
  public boolean readBoolean() {
    return this.readInteger() != 0;
  }

  @Override
  public void skip() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int readValueDictionaryId() {
    return readInteger();
  }

  @Override
  public int readInteger() {
    if (this.currentCount == 0) {
      this.readNextGroup();
    }

    this.currentCount--;
    switch (mode) {
      case RLE:
        return this.currentValue;
      case PACKED:
        return this.packedValuesBuffer[packedValuesBufferIdx++];
    }
    throw new RuntimeException("Unrecognized mode: " + mode);
  }
}
