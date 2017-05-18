/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.code.or.io.impl;

import com.google.code.or.common.glossary.UnsignedLong;
import com.google.code.or.common.glossary.column.BitColumn;
import com.google.code.or.common.glossary.column.StringColumn;
import com.google.code.or.common.util.CodecUtils;
import com.google.code.or.io.ExceedLimitException;
import com.google.code.or.io.XInputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * This class will be used by XDeserializer to read data from the input byte array.
 *
 * @author Litao Deng
 */
public class XByteArrayInputStream extends InputStream implements XInputStream
{
  private InputStream inputStream;
  private int readCount = 0;
  private int readLimit = 0;

  /**
   * The underlying ByteArrayInputStream will directly point to the input
   * byte array instead of allocating a new one.
   */
  public XByteArrayInputStream(byte[] data) {
    this.inputStream = new ByteArrayInputStream(data);
  }

  @Override
  public void close() throws IOException {
    this.inputStream.close();
  }

  @Override
  public int available() throws IOException {
    if (this.readLimit > 0) {
      return this.readLimit - this.readCount;
    } else {
      return this.inputStream.available();
    }
  }

  public boolean hasMore() throws IOException {
    return this.inputStream.available() > 0;
  }

  public void setReadLimit(final int limit) throws IOException {
    this.readCount = 0;
    this.readLimit = limit;
  }

  @Override
  public long skip(final long n) throws IOException {
    if (this.readLimit > 0 && (this.readCount + n) > this.readLimit) {
      this.readCount += this.inputStream.skip(this.readLimit - this.readCount);
      throw new ExceedLimitException();
    } else {
      this.readCount += this.inputStream.skip(n);
      return n;
    }
  }

  @Override
  public int read() throws IOException {
    if (this.readLimit > 0 && (this.readCount + 1) > this.readLimit) {
      throw new ExceedLimitException();
    } else {
      final int r = this.inputStream.read() & 0xFF;
      ++this.readCount;
      return r;
    }
  }

  public byte[] readBytes(final int length) throws IOException {
    final byte[] r = new byte[length];
    fill(r, 0, length);
    return r;
  }

  public void fill(final byte[] b, final int off, final int len) throws IOException {
    if (this.readLimit > 0 && (this.readCount + len) > this.readLimit) {
      this.readCount += read(b, off, this.readLimit - this.readCount);
      throw new ExceedLimitException();
    } else {
	   int data = this.inputStream.read(b, off, len);
      this.readCount += (data == -1 ? 0 : data)
    }
  }

  public StringColumn readLengthCodedString() throws IOException {
    final UnsignedLong length = readUnsignedLong();
    return length == null ? null : readFixedLengthString(length.intValue());
  }

  public StringColumn readNullTerminatedString() throws IOException {
    ByteArrayOutputStream s = new ByteArrayOutputStream();
    for (int b; (b = this.read()) != 0; ) {
      s.write(b);
    }
    return StringColumn.valueOf(s.toByteArray());
  }

  public StringColumn readFixedLengthString(final int length) throws IOException {
    return StringColumn.valueOf(readBytes(length));
  }

  public int readSignedInt(int length) throws IOException {
    int r = 0;
    for (int i = 0; i < length; ++i) {
      final int v = this.read();
      r |= (v << (i << 3));
      if ((i == length - 1) && ((v & 0x80) == 0x80)) {
        for (int j = length; j < 4; ++j) {
          r |= (255 << (j << 3));
        }
      }
    }
    return r;
  }

  public long readSignedLong(int length) throws IOException {
    long r = 0;
    for (int i = 0; i < length; ++i) {
      final long v = this.read();
      r |= (v << (i << 3));
      if ((i == length - 1) && ((v & 0x80) == 0x80)) {
        for (int j = length; j < 8; ++j) {
          r |= (255 << (j << 3));
        }
      }
    }
    return r;
  }

  public int readInt(int length) throws IOException {
    int r = 0;
    for (int i = 0; i < length; ++i) {
      r |= (this.read() << (i << 3));
    }
    return r;
  }

  public long readLong(int length) throws IOException {
    long r = 0;
    for (int i = 0; i < length; ++i) {
      r |= ((long)this.read() << (i << 3));
    }
    return r;
  }

  public int readInt(int length, boolean littleEndian) throws IOException {
    int r = 0;
    for (int i = 0; i < length; ++i) {
      final int v = this.read();
      if (littleEndian) {
        r |= (v << (i << 3));
      } else {
        r = (r << 8) | v;
      }
    }
    return r;
  }

  public long readLong(int length, boolean littleEndian) throws IOException {
    long r = 0;
    for (int i = 0; i < length; ++i) {
      final long v = this.read();
      if (littleEndian) {
        r |= (v << (i << 3));
      } else {
        r = (r << 8) | v;
      }
    }
    return r;
  }

  public UnsignedLong readUnsignedLong() throws IOException {
    final int v = this.read();
    if (v < 251) return UnsignedLong.valueOf(v);
    else if (v == 251) return null;
    else if (v == 252) return UnsignedLong.valueOf(readInt(2));
    else if (v == 253) return UnsignedLong.valueOf(readInt(3));
    else if (v == 254) return UnsignedLong.valueOf(readLong(8));
    else throw new RuntimeException("assertion failed, should NOT reach here");
  }

  public BitColumn readBit(int length) throws IOException {
    return readBit(length, true);
  }

  public BitColumn readBit(int length, boolean littleEndian) throws IOException {
    byte[] bytes = readBytes((length + 7) >> 3);
    if (!littleEndian) bytes = CodecUtils.toBigEndian(bytes);
    return BitColumn.valueOf(length, bytes);
  }
