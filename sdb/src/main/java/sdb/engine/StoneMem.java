/**
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package sdb.engine;

import sun.misc.Unsafe;
import java.lang.reflect.Field;
import java.util.UUID;

@SuppressWarnings("restriction")
class StoneMem {
  public static final Unsafe unsafe;
  
  static {
    try {
      Field field = Unsafe.class.getDeclaredField("theUnsafe");
      field.setAccessible(true);
      unsafe = (Unsafe)field.get(null);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private StoneMem() {}
  
  private static final StoneMem _mem = new StoneMem();
  public static StoneMem get() { return _mem; }

  public long allocateMemory(long size) {
    long address = unsafe.allocateMemory(size); 
    unsafe.setMemory(address, size, (byte)0); 
    return address;
  }

  public long reallocateMemory(long address, long oldBytes, long bytes) {
    address = unsafe.reallocateMemory(address, bytes);
    if (bytes > oldBytes)  
      unsafe.setMemory(address + oldBytes, (bytes-oldBytes), (byte)0); 
    
    return address;
  }

  public int fieldOffset(Field f) {
    return unsafe.fieldOffset(f);
  }

  public void copyMemory(long srcAddress, long destAddress, long bytes) {
    unsafe.copyMemory(srcAddress, destAddress, bytes);
  }

  public void freeMemory(long address) {
    unsafe.freeMemory(address);
  }

  public void setMemory(long address, long bytes, byte value) {
    unsafe.setMemory(address, bytes, value);
  }

  public int addressSize() {
    return unsafe.addressSize();
  }

  public byte getByte(long address) {
    return unsafe.getByte(address);
  }

  public void putByte(long address, byte val) {
    unsafe.putByte(address, val);
  }
  
  public char getChar(long address) {
    return unsafe.getChar(address);
  }

  public void putChar(long address, char val) {
    unsafe.putChar(address, val);
  }

  public short getShort(long address) {
    return unsafe.getShort(address);
  }

  public void putShort(long address, short val) {
    unsafe.putShort(address, val);
  }

  public int getInt(long address) {
    return unsafe.getInt(address);
  }

  public void putInt(long address, int val) {
    unsafe.putInt(address, val);
  }

  public float getFloat(long address) {
    return unsafe.getFloat(address);
  }

  public void putFloat(long address, float val) {
    unsafe.putFloat(address, val);
  }

  public long getLong(long address) {
    return unsafe.getLong(address);
  }

  public long getLong(Object o, long offset) {
    return unsafe.getLong(o, offset);
  }

  public void putLong(long address, long val) {
    unsafe.putLong(address, val);
  }

  public double getDouble(long address) {
    return unsafe.getDouble(address);
  }

  public void putDouble(long address, double value) {
    unsafe.putDouble(address, value);
  }
}

