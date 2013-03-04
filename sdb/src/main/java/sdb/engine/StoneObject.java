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

import java.util.UUID;

@SuppressWarnings("restriction")
public class StoneObject {
  protected long address;

  static final StoneMem mem = StoneMem.get();
  static long offset = 0;
  static final long metaOffset = offset; 
  static final long aOffset = offset += 1; 
  static final long typeOffset = offset += 1; 
  static final long attrOffset = offset += 1; 
  static final long refCountOffset = offset += 1; 
  static final long scalarOffset = offset += 4; 
  static final long scalarObjectSize = offset+8;
  static final long listOffset = offset += 8; 

  public static int PrintSize = 30;

  public static class SException extends StoneLisp.LangException {
    public SException(String msg) {
      super(msg);
    }
    public SException(Exception e) {
      super(e);
    }
  }

  public static final StoneObject True = StoneObject.alloc(1);
  public static final StoneObject False = StoneObject.alloc(0);
  public static final StoneObject Null = new StoneObject();

  public static enum Type { 
    
    private final byte type;

    Type(byte type) {
      this.type = type; 
    }

    public byte asList() {
      return type;
    }

    public byte asScalar() {
      return (byte)-type;
    }

    public static int[] bitShifts = {0,0,1,1,2,2,3,3,3,3};
    public static Type getType(byte t) {
      if (t < 0) 
        t = (byte)-t;
      switch (t) {
      case 0:
        return Null;
      case 1:
        return Byte;
      case 2:
        return Char;
      case 3:
        return Short;
      case 4:
        return Int;
      case 5:
        return Float;
      case 6:
        return Double;
      case 7:
        return Long;
      case 8:
        return Symbol;
      case 9:
        return List;
      }
      throw new SException("type");
    }

    public static int getTypeSize(byte t) {
      switch (t) {
      case 0:
        return 0;
      case 1:
        return 1;
      case 2:
        return 2;
      case 3:
        return 2;
      case 4:
        return 4;
      case 5:
        return 4;
      case 6:   
        return 8;
      case 7:
        return 8;
      case 8:
        return  mem.addressSize(); 
      case 9:
        return mem.addressSize();  
      case 10:
        return 16;
      }
      return  mem.addressSize();
    }
    
    public static int getTypeSize(Type t) {
      switch (t) {
      case Null:
        return 0;
      case Byte:
        return 1;
      case Char:
        return 2;
      case Short:
        return 2;
      case Int:
        return 4;
      case Float:
        return 4;
      case Double:
        return 8;
      case Long:
        return 8;
      case List:
        return mem.addressSize();  
      }
      throw new SException("type");
    }
  }

  long address() {
    return address;
  }

  long dataStart() {
    return address + scalarOffset;
  }

  public void writeFields() {
  }

  public void readFields() {
  }

    public long length() {
      return mem.getInt(address + scalarOffset);
    }

    public long getByteSize() {
      return getRawByteSize();
    }

    public long getRawByteSize() {
      return scalarObjectSize+4+length()*2;
    }

    private long byteLength(long n) {
      return n << 1;
    }

    private char getChar(long pos) {
      if (pos >= length()) 
        return ' ';
      return mem.getChar(dataStart() + byteLength(pos));
    }

    public static boolean equals(Symbol str1, Symbol str2) {
      long len = str1.length();
      if (len == str2.length()) {
        for (long i = 0; i < len; i++) {
          if (str1.getChar(i) != str2.getChar(i))
            return false;
        }
        return true;
      }
      return false;
    }

    public boolean equals(Object obj) {
      if (obj instanceof Symbol) 
        return equals(this, (Symbol)obj);
      else if (obj instanceof StoneObject) {
        StoneObject so = (StoneObject)obj;
        if (so.getType() == (byte)(Type.Symbol.asScalar())) 
          return equals(this, (Symbol)so);
      }
      return false;
    }

    public int hashCode() {
      if (length() <=0) 
        return 0;
      long dataStart = dataStart();
      int h = getChar();
      for (long i = 1; i < length(); i++)
        h = (h << 5) - h + getChar(i); 
      return h;
    }

    public java.lang.String toString() {
      StringBuffer buf = new StringBuffer(); 
      for (long i  = 0; i < length(); i++) {
        buf.append(getChar(i));
      }
      return buf.toString();
    }
  }

  public StoneObject() {
    address = mem.allocateMemory(scalarObjectSize);
    retain();
  }

  StoneObject(long address) {
    this.address = address;
  }

  public StoneObject(byte type) {
    if (type > 0) 
      throw new SException("type");
    address = mem.allocateMemory(scalarObjectSize);

    mem.putByte(address+typeOffset, type);
    retain();
  }

  public StoneObject(byte type, StoneList.Byte buf) {
    address = mem.allocateMemory(scalarObjectSize);
    mem.putByte(address+typeOffset, type);
    putLong(buf.getLong(scalarOffset));
    retain();
  }

  static StoneObject build(long address) {
    if (address == 0) 
      return StoneObject.Null;
    
    byte t = mem.getByte(address+typeOffset);
    if (t<=0) {
      if (t == Type.Symbol.asScalar())  
        return new Symbol(address);
      else 
        return new StoneObject(address);
    }

    byte m = t;
    switch (t) {
    case 0:
      throw new SException("nyi");
    case 1:
      throw new SException("nyi");
    case 2:
      return new StoneList.String(address, m);
    case 3:
      throw new SException("nyi");
    case 4:
      return new StoneList.Int(address, m);
    case 5:
      throw new SException("nyi");
    case 6:
      return new StoneList.Double(address, m);
    case 7:
      return new StoneList.Long(address, m);
    case 8:
      throw new SException("nyi");
    case 9:
      return new StoneList(address, m);
    }
    if (t>=30&&t<=97) 
      return new StoneEnum(address);
    throw new SException("nyi:" + t);
  }

  static StoneObject clone(long address) {
    if (address == 0) 
      return StoneObject.Null;

    byte t = mem.getByte(address+typeOffset);
    if (t<=0) {
      if (t == Type.Symbol.asScalar())  
        return Symbol.clone(address);
      else {
        StoneObject so = new StoneObject(t); 
        so.putLong(mem.getLong(address+scalarOffset));
        return so;
      }
    }

    StoneObject so = new StoneObject(address); 
    long size = so.getByteSize();
    long newaddress = mem.allocateMemory(size);
    mem.copyMemory(address, newaddress, size);
    clearRefCount(newaddress);
    
    so = new StoneObject(newaddress); 
    
    resetSizeField(newaddress);

    switch (t) {
    case 0:
      throw new SException("nyi");
    case 1:
      throw new SException("nyi");
    case 2:
      return new StoneList.String(address, t);
    case 3:
      throw new SException("nyi");
    case 4:
      return new StoneList.Int(address, t);
    case 5:
      throw new SException("nyi");
    case 6:
      return new StoneList.Double(address, t);
    case 7:
      return new StoneList.Long(address, t);
    case 9:
      return new StoneList(address, t);
    }
    throw new SException("nyi:" + t);
  }

  public static StoneObject alloc(byte val) {
    StoneObject so = new StoneObject(Type.Byte.asScalar());
    so.putByte(val);
    return so;
  }

  public static StoneObject alloc(char val) {
    StoneObject so = new StoneObject(Type.Char.asScalar());
    so.putChar(val);
    return so;
  }

  public static StoneObject alloc(short val) {
    StoneObject so = new StoneObject(Type.Short.asScalar());
    so.putShort(val);
    return so;
  }

  public static StoneObject alloc(int val) {
    StoneObject so = new StoneObject(Type.Int.asScalar());
    so.putInt(val);
    return so;
  }

  public static StoneObject alloc(float val) {
    StoneObject so = new StoneObject(Type.Float.asScalar());
    so.putFloat(val);
    return so;
  }

  public static StoneObject alloc(double val) {
    StoneObject so = new StoneObject(Type.Double.asScalar());
    so.putDouble(val);
    return so;
  }
  

  public static StoneObject alloc(long val) {
    StoneObject so = new StoneObject(Type.Long.asScalar());
    so.putLong(val);
    return so;
  }

  public StoneObject(byte type, long size) {
    if (type < 0) 
      address = mem.allocateMemory(size);
    else {
      address = mem.allocateMemory(listOffset + (size * Type.getTypeSize(type)));
      mem.putLong(address+listObjectLength, size);
      mem.putLong(address+listObjectSize, size);
    }
    mem.putByte(address+typeOffset, type);
    retain();
  }

  public void retain() {
    int ref = mem.getInt(address+refCountOffset);
    ref++;
    mem.putInt(address+refCountOffset, ref);
  }

  public byte getMeta() {
    return  mem.getByte(address+metaOffset); 
  }

  protected void putMeta(byte b) {
    mem.putByte(address+metaOffset, b);
  }

  private static void resetSizeField(long address) {
    mem.putLong(address+listObjectSize, mem.getLong(address+listObjectLength));
  }

  private static void clearRefCount(long address) {
    mem.putInt(address+refCountOffset, 1); 
  }

  int refCount() {
    return mem.getInt(address+refCountOffset);
  }

  protected int decRefCount() {
    int ref = mem.getInt(address+refCountOffset);
    --ref;
    mem.putInt(address+refCountOffset, ref);
    return ref;
  }

  public void release() {
    if (address == 0 || this == StoneObject.Null || this == StoneObject.True || this == StoneObject.False || this == StoneList.Empty)
      return;
    if (address == -1) 
      throw new SException("invalid object");
    int ref = decRefCount(); 
    if (ref <= 0) 
      free();
  }

  protected long byteOffset(long n) {
    return n << Type.bitShifts[getType()]; 
  }

  protected void free() {
    assert getRefcount() == 0 : "ref counting error";
    if (address != -1) {
      byte t = getType();
      if (t<Type.List.asList() || t == StoneLisp.Type.Dictionary.asList())
        mem.freeMemory(address);
      else {
        for (long pos = 0; pos < length(); pos++) {
          new StoneObject(mem.getLong(address+listOffset+ byteOffset(pos))).release();
        }
        mem.freeMemory(address);
      }
      address = -1;
    }
  }

  public int getRefcount() {
    return mem.getInt(address+refCountOffset);
  }

  public long getRawByteSize() {
    byte t = getType();
    if (t < 0) 
      return scalarObjectSize;
    return listOffset + Type.getTypeSize(getType())*length();
  }

  public long getByteSize() {
    return getRawByteSize();
  }

  public long length() {
    if (getType() < 0) {
      return 0;
    }
    return mem.getLong(address + listObjectLength);
  }

  public long size() {
    return 0;
  }

  public Type type() {
    return Type.getType(getType());
  }

  public boolean isNull() {
    return this == StoneObject.Null;
  }

  public boolean isScalar() {
    return getType() <= 0;
  }

  public boolean isList() {
    return getType() > 0;
  }

  public byte getType() {
    return mem.getByte(address+typeOffset);
  }

  void putType(byte t) {
    mem.putByte(address+typeOffset, t);
  }

  private void checkScalar() {
    if (isList()) 
      throw new SException("type");
  }

  public byte getByte() {
    checkScalar();
    return mem.getByte(dataStart());
  }

  public void putByte(byte val) {
    checkScalar();
    mem.putByte(dataStart(), val);
  }
  
  public char getChar() {
    checkScalar();
    return mem.getChar(dataStart());
  }

  public void putChar(char val) {
    checkScalar();
    mem.putChar(dataStart(), val);
  }

  public short getShort() {
    checkScalar();
    return mem.getShort(dataStart());
  }

  public void putShort(short val) {
    checkScalar();
    mem.putShort(dataStart(), val);
  }

  public int getInt() {
    checkScalar();
    return mem.getInt(dataStart());
  }

  public void putInt(int val) {
    checkScalar();
    mem.putInt(dataStart(), val);
  }

  public float getFloat() {
    checkScalar();
    return mem.getFloat(dataStart());
  }

  public void putFloat(float val) {
    checkScalar();
    mem.putFloat(dataStart(), val);
  }

  public long getLong() {
    checkScalar();
    return mem.getLong(dataStart());
  }

  public void putLong(long val) {
    checkScalar();
    mem.putLong(dataStart(), val);
  }

  public double getDouble() {
    checkScalar();
    return mem.getDouble(dataStart());
  }

  public void putDouble(double val) {
    checkScalar();
    mem.putDouble(dataStart(), val);
  }

  public double asDouble() {
    byte t = getType();
    if (t <= 0) {
      t = (byte)-t;
      switch (t) {
      case 0:
        return getByte();
      case 1:
        return getByte();
      case 3:
        return getShort();
      case 4:
        return getInt();
      case 5:
        return getFloat();
      case 6:
        return getDouble();
      }
    }
    throw new SException("wrong type");
  }

  public long asLong() {
    byte t = getType();
    if (t <= 0) {
      t = (byte)-t;
      switch (t) {
      case 0:
        return getByte();
      case 1:
        return getByte();
      case 3:
        return getShort();
      case 4:
        return getInt();
      case 5:
        return (long)getFloat();
      case 6:
        return (long)getDouble();
      case 7:
        return getLong();
      }
    }
    throw new SException("wrong type");
  }

  public int asInt() {
    byte t = getType();
    if (t <= 0) {
      t = (byte)-t;
      switch (t) {
      case 0:
        return getByte();
      case 1:
        return getByte();
      case 3:
        return getShort();
      case 4:
        return getInt();
      case 5:
        return (int)getFloat();
      case 6:
        return (int)getDouble();
      case 7:
        return (int)getLong();
      }
    }
    throw new SException("wrong type");
  }

  public int hashCode() {
    byte t = getType();
    if (t < 0) {
      t = (byte)-t;
      switch (t) {
      case 0:
        return getByte();
      case 1:
        return getByte();
      case 2:
        return getChar();
      case 3:
        return getShort();
      case 4:
        return getInt();
      case 5:
        return (int)getFloat();
      case 6:
        return (int)getDouble();
      case 7:
        long n = getLong();
        return (int)((n>>32)^n);
      case 8:
        return ((Symbol)this).hashCode();
      case 9:
        return ((StoneList)build(address)).hashCode();
      }
    }
    throw new SException("wrong type");
  }

  public boolean equals(Object obj) {
    if (obj instanceof StoneObject) {
      byte t = getType();
      StoneObject so = (StoneObject)obj;
      if (t <= 0) {
        byte t2= so.getType();
        if (t == t2) {
          t = (byte)-t;
          switch (t) {
          case 0:
            return getByte() == so.getByte();
          case 1:
            return getByte() == so.getByte();
          case 2:
            return getChar() == so.getChar();
          case 3:
            return getShort() == so.getShort();
          case 4:
            return getInt() == so.getInt();
          case 5:
            return getFloat() == so.getFloat();
          case 6:
            return getDouble() == so.getDouble();
          case 7:
            return getLong() == so.getLong();
          default:
            throw new SException("nyi");
          }
        } else if (t2<0 && t2!=-2 && t2!=-8) {
          if (asLong() == so.asLong() || asDouble() == so.asDouble())
            return true;
        }
        return false;
      }
      switch (t) {
      case 0:
        throw new SException("nyi");
      case 1:
        throw new SException("nyi");
      case 2:
        return new StoneList.String(this.address, t).equals(so);
      case 3:
        throw new SException("nyi");
      case 4:
        return new StoneList.Int(this.address, t).equals(so);
      case 5:
        throw new SException("nyi");
      case 6:
        return new StoneList.Double(this.address, t).equals(so);
      case 7:
        throw new SException("nyi");
      case 8:
        throw new SException("nyi");
      case 9:
        throw new SException("nyi");
      default:
        throw new SException("nyi");
      }    
    }
    return false;
  }

  public StoneObject eval(StoneLisp.Environment env, StoneList args) {
    retain();
    return this;
  }

  public StoneObject eval() {
    retain();
    return this;
  }

  public StoneObject first() {
    if (this == Null) 
      return Null;
    if (this == True)
      return True;
    if (this == False)
      return False;
    return clone(this.address);
  }

  public StoneObject rest() {
    if (this == Null || this == True || this == False) 
      return Null;
    return Null; 
  }

  public StoneObject add(StoneObject obj) {
    byte t1 = getType(); 
    byte t2 = obj.getType();
    if (t1 < 0) {
      t1 = (byte)-t1;
      if (t2 > 0) 
        throw new StoneLisp.LangException("nyi");
      switch (t1) {
      case 0:
        throw new StoneLisp.LangException("type");
      case 1:
        return StoneObject.alloc(getByte() + obj.getByte());
      case 2:
        throw new StoneLisp.LangException("type");
      case 3:
        return StoneObject.alloc(getShort() + obj.getShort());
      case 4:
        return StoneObject.alloc(getInt() + obj.getInt());
      case 5:
        return StoneObject.alloc(getFloat() + obj.getFloat());
      case 6:
        return StoneObject.alloc(getDouble() + obj.getDouble());
      case 7:
        return StoneObject.alloc(getLong() + obj.getLong());
      }
    }
    throw new StoneLisp.LangException("nyi");
  }

  public StoneObject minus(StoneObject obj) {
    byte t1 = getType(); 
    byte t2 = obj.getType();
    if (t1 < 0) {
      t1 = (byte)-t1;
      if (t2 > 0) 
        throw new StoneLisp.LangException("nyi");
      switch (t1) {
      case 0:
        throw new StoneLisp.LangException("type");
      case 1:
        return StoneObject.alloc(getByte() - obj.getByte());
      case 2:
        throw new StoneLisp.LangException("type");
      case 3:
        return StoneObject.alloc(getShort() - obj.getShort());
      case 4:
        return StoneObject.alloc(getInt() - obj.getInt());
      case 5:
        return StoneObject.alloc(getFloat() - obj.getFloat());
      case 6:
        return StoneObject.alloc(getDouble() - obj.getDouble());
      case 7:
        return StoneObject.alloc(getLong() - obj.getLong());
      }
    }
    throw new StoneLisp.LangException("nyi");
  }

  public StoneList getList(StoneList.Long index) {
    throw new SException("type");
  }

  public StoneList getList(StoneList.Int index) {
    throw new SException("type");
  }

  public StoneList join(StoneObject o) {
    throw new StoneLisp.LangException("not implemented");
  }

  public StoneList join(StoneList list) {
    throw new StoneLisp.LangException("not implemented");
  }


  public String toString() {
    if (this == Null) 
      return "nil";
    else if (this == True) 
      return "1";
    else if (this == False)
      return "0";
    byte t = getType();
    if (t <= 0) {
      StringBuffer buf = new StringBuffer();
      t = (byte)-t;
      switch (t) {
      case 0:
        buf.append(getByte());
        break;
      case 1:
        buf.append(getByte());
        break;
      case 2:
        buf.append(getChar());
        break;
      case 3:
        buf.append(getShort());
        break;
      case 4:
        buf.append(getInt());
        break;
      case 5:
        buf.append(getFloat());
        break;
      case 6:
        buf.append(getDouble());
        break;
      case 7:
        buf.append(getLong());
        break;
      case 8:
        return ((Symbol)(this)).toString();
      default:
        throw new SException("type:" + t);
      }
      return buf.toString();
    }

    switch (t) {
    case 1:
      throw new SException("nyi");
    case 2:
      return new StoneList.String(this.address, t).toString();
    case 3:
      throw new SException("nyi");
    case 4:
      return new StoneList.Int(this.address, t).toString();
    case 5:
      throw new SException("nyi");
    case 6:
      return new StoneList.Double(this.address, t).toString();
    case 7:
      throw new SException("nyi");
    case 8:
      throw new SException("nyi");
    case 9:
      throw new SException("nyi");
    default:
      throw new SException("nyi");
    }    
  }

  public void putObject(StoneList.Byte data) {
    
  }
}
