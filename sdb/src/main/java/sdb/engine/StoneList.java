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

import java.util.Formatter;
import java.util.Locale;

public class StoneList extends StoneObject {

  public static final StoneList Empty = new StoneList(); 

  public StoneList() {
    this(StoneObject.Type.List.asList(), 0);
  }

  public StoneList(long size) {
    this(StoneObject.Type.List.asList(), size);
  }

  StoneList(long address, byte m) {
    this.address = address;
  }

  protected StoneList(byte type, long length) {
    super(type, length);
    mem.putLong(address+listObjectSize, length);
    mem.putLong(address+listObjectLength, length);
  }

  protected StoneList(byte type, long size, long length) {
    super(type, size);
    mem.putLong(address+listObjectSize, size);
    mem.putLong(address+listObjectLength, length);
  }

  public void setLength(long len) {
    if (len > size()) 
      return;
    putLength(len);
  }

  long dataStart() {
    return address + listOffset;
  }

  protected void putLength(long length) {
    mem.putLong(address+listObjectLength, length);
  }

  protected void incLength() {
    mem.putLong(address+listObjectLength, length()+1);
  }

  protected void decLength() {
    mem.putLong(address+listObjectLength, length()-1);
  }

  protected void incLengthBy(long len) {
    mem.putLong(address+listObjectLength, length()+len);
  }

  public boolean isNull() {
    return this == StoneList.Empty;
  }

  public long size() {
    return mem.getLong(address+listObjectSize);
  }

  protected void incSize() {
    long size = size();
    size += (size >> 3) + (size < 9 ? 3 : 6);
    mem.putLong(address+listObjectSize, size);
  }

  protected void incSizeBy(long inc) {
    long size = size();
    long oldSize = size;
    size += (size >> 3) + (size < 9 ? 3 : 6);
    if ((size - oldSize) < inc)
      size += inc;
    mem.putLong(address+listObjectSize, size);
  }

  protected void checkLength(long n) {
    if (n >= length()) 
      throw new SException("length:" + n);
  }

  protected void realloc() {
    long oldBytes = listOffset + Type.getTypeSize(getType())*size();
    incSize();
    address = mem.reallocateMemory(address, oldBytes, listOffset + (Type.getTypeSize(getType())*size()));
  }

  public void resizeBy(long inc) {
    long oldBytes = listOffset + Type.getTypeSize(getType())*size();
    incSizeBy(inc);
    address = mem.reallocateMemory(address, oldBytes, listOffset + (Type.getTypeSize(getType())*size()));
  }

  public void resize(long newSize) {
    if (newSize <= 0) 
      throw new SException("size:" + newSize);
    long oldBytes = listOffset + Type.getTypeSize(getType())*size();
    mem.putLong(address+listObjectSize, newSize);
    mem.putLong(address+listObjectLength, newSize);
    address = mem.reallocateMemory(address, oldBytes, listOffset + (Type.getTypeSize(getType())*newSize));
  }

  public void setValue(long bytes, byte value) {
    mem.setMemory(dataStart(), bytes, value);
  }

  public long length() {
    return mem.getLong(address+listObjectLength);
  }
  
  public long getByteSize() {
    long size = super.getByteSize();
    for (long i = 0; i < length(); i++) {
      StoneObject so = getObject(i);
      size += so.getByteSize();
    }
    return size;
  }

  public boolean equals(long pos1, long pos2) {
    return getObject(pos1).equals(getObject(pos2));
  }

  public boolean equals(long pos, StoneObject obj) {
    return getObject(pos).equals(obj);
  }

  public static boolean equals(StoneList list1, StoneList list2) {
    if (list1.length() != list2.length()) 
      return false;
    for (long pos = 0; pos < list1.length(); pos++) {
      if (!list1.getObject(pos).equals(list2.getObject(pos)))
        return false;
    }
    return true;
  }

  public boolean equals(Object obj) {
    if (obj instanceof StoneList) {
      byte t = getType();
      StoneObject so = (StoneObject)obj;
      if (t != so.getType())
        return false;
      switch (t) {
      case 0:
        throw new SException("nyi");
      case 1:
        throw new SException("nyi");
      case 2:
        return StoneList.String.equals((StoneList.String)this, (StoneList.String)so);
      case 3:
        throw new SException("nyi");
      case 4:
        return StoneList.Int.equals((StoneList.Int)this, (StoneList.Int)so);
      case 5:
        throw new SException("nyi");
      case 6:
        return StoneList.Double.equals((StoneList.Double)this, (StoneList.Double)so);
      case 7:
        throw new SException("nyi");
      case 8:
        return StoneList.Symbol.equals((StoneList.Symbol)this, (StoneList.Symbol)so);
      case 9:
        return equals(this, (StoneList)obj);
      default:
        throw new SException("nyi");
      }    
    }
    return false;
  }

  private long byteLength(long n) {
    return n << 3;
  }

  public long shallowFind(StoneObject obj) {
    for (long n = 0; n < length(); n++) {
      long adr = mem.getLong(dataStart() + byteOffset(n));
      if (adr == obj.address)
        return n;
    }
    return length();
  }

  public StoneObject getObject(long n) {
    checkLength(n);     
    if (n >= length()) 
      return StoneObject.Null;
    return build(mem.getLong(dataStart() + byteOffset(n)));
  }

  protected long peekLong(long n) {
    return mem.getLong(dataStart() + byteOffset(n));
  }

  protected void pokeLong(long n, long d) {
    long pos = dataStart() + byteOffset(n);
    mem.putLong(pos, d);
  }

  public void putObject(long n, StoneObject obj) {
    checkLength(n);    
    long pos = dataStart() + byteOffset(n);
    build(mem.getLong(pos)).release();
    mem.putLong(pos, obj.address);
    obj.writeFields();
    obj.retain();
  }

  private StoneObject applyLambda(StoneLisp.Environment env, StoneObject so, StoneObject args) {
    StoneObject ret = StoneObject.Null;

    StoneObject param = so.first();
    if (param instanceof StoneObject.Symbol || param.getType() == Type.Symbol.asScalar()) {
      param.release();
      so.retain();
      return so;
    }

    StoneObject body = so.rest();
    StoneLisp.Lambda lambda = new StoneLisp.Lambda(param, body);
    param.release();
    body.release();
    StoneObject rest = args.rest();
    StoneObject params = rest.first();
    ret = StoneObject.Null;
    try {
      ret = lambda.eval(env, params);
    } catch (StoneLisp.LangException e) {
      env.debugError(e);
    }
    params.release();
    rest.release();
    lambda.release();
    return ret;
  }
  
  public StoneObject eval(StoneLisp.Environment env, StoneList args) {
    StoneObject ret = StoneObject.Null;
    StoneObject so;
    byte t = getType();

    if (t!=2 && t <= 7 || t == 10) {
      retain();
      return this;
    }
    if (this instanceof StoneList.String) {
      so = env.get(this);
      if (so == StoneObject.Null)
        so = this;
      so.retain();
      return so;
    }
    StoneObject sym = getObject(0); 
    if (sym.isScalar()) {
      if (length() == 1) {
        sym.retain();
        return sym;
      }
      retain();
      return this;
    }
    if (sym.getType() == Type.List.asList())  {
      StoneObject head = sym.first();
      if (head instanceof StoneObject.Symbol) {
        retain();
        return this;
      }
      so = env.get(head);
      head.release();
      if (so instanceof StoneLisp.PrimitiveFunction) {
        StoneList params = (StoneList)(sym.rest());
        try {
          ret = so.eval(env, params);
        } catch (StoneLisp.LangException e) {
          env.debugError(e);
        }
        params.release();
        return ret;
      } else {
        if (so instanceof StoneList.Lambda || so.getType() == StoneLisp.Type.Lambda.asByte()) 
          ret= so.eval(env, (StoneList)sym);
        else {
          StoneList list = new StoneList(length());
          for (long i= 0; i < length(); i++) 
            list.putObject(i, StoneFuncs.evalObject(env, getObject(i)));

          ret = list; 
        }
        return ret;
      }
    } 
    so = StoneFuncs.evalObject(env, sym); 
    if (so == StoneObject.Null) {
      this.retain();
      return this;
    }
    if (so instanceof StoneLisp.PrimitiveFunction) {
      StoneList params = (StoneList)(rest());
      try {
        ret = so.eval(env, params); 
      } catch (StoneLisp.LangException e) {
        env.debugError(e);
      }
      params.release();
      return ret;
    } else {
      if (so.isScalar()) { 
        so.retain();
        return so;
      }
      
      if (so instanceof String) 
        return so.eval(env, null);

      if (so instanceof StoneList.Lambda || so.getType() == StoneLisp.Type.Lambda.asByte()) 
        return so.eval(env, this);
      if (length() == 1) 
        return so; 
      else {
        StoneList list = new StoneList(length());
        list.putObject(0, so);
        for (long i= 1; i < length(); i++) 
          list.putObject(i, StoneFuncs.evalObject(env, getObject(i)));
        return list;
      }
    }
  }
  

  public StoneObject first() {
    if (length() == 0) 
      return StoneObject.Null;
    return clone(mem.getLong(dataStart()));
  }

  public StoneObject rest() {
    long len = length();
    if (len < 1) 
      return StoneObject.Null;
    StoneList list = new StoneList(getType(), 0);
    for (long i = 1; i < len; i++) {
      StoneObject so = build(mem.getLong(dataStart() + byteOffset(i)));
      list.append(so);
    }
    return list; 
  }

  public void appendList(StoneList list) {
    if (list instanceof StoneList.Int)
      appendList((StoneList.Int)list);
    else if (list instanceof StoneList.Long)
      appendList((StoneList.Long)list);
    else if (list instanceof StoneList.Double)
      appendList((StoneList.Double)list);
    else if (list instanceof StoneEnum)
      appendList((StoneEnum)list);
    else 
      throw new SException("nyi");
  }

  public void appendList(StoneList.Int list) {
    throw new SException("nyi");
  }

  public void appendList(StoneList.Long list) {
    throw new SException("nyi");
  }

  public void appendList(StoneList.Double list) {
    throw new SException("nyi");
  }

  public void appendList(StoneEnum list) {
    throw new SException("nyi");
  }

  public void append(StoneObject val) {
    long length = length();
    if (length >= size()) 
      realloc();
    incLength();
    putObject(length, val);
    return;
  }

  public void append(int val) {
    throw new SException("nyi");
  }

  public void append(long val) {
    throw new SException("nyi");
  }

  public void append(double val) {
    throw new SException("nyi");
  }

  public void append(Object val) {
    if (!(val instanceof StoneObject))
      throw new SException("not supported");
    append((StoneObject)val);
  }

  public int hashCode(long pos) {
    return getObject(pos).hashCode();
  }

  public int hashCode() {
    long len = length();

    len = len > PrintSize? PrintSize : len;
    int hash = getObject(0).hashCode();
    for (long i = 1; i < len; i++) 
      hash ^= getObject(i).hashCode();

    return hash;
  }

  public java.lang.String toString(long pos) {
    return getObject(pos).toString();
  }

  public java.lang.String toString() {
    StringBuffer buf = new StringBuffer();
    buf.append("(");
    for (long i = 0; i < length(); i++) {
      StoneObject so = getObject(i);
      buf.append(so);
      buf.append(" ");
    }
    buf.append(")");
    return buf.toString();
  }

  public StoneObject add(StoneObject obj) {
    throw new SException("nyi");
  }

  public void add(long pos, StoneObject obj) {
    throw new SException("nyi");
  }

  public void addObject(StoneList srclist, long srcPos, long dstPos) {
    throw new SException("nyi");
  }

  public int getInt(long n) {
    throw new SException("nyi");
  }

  public long getLong(long n) {
    throw new SException("nyi");
  }

  public double getDouble(long n) {
    throw new SException("nyi");
  }

  public void copyObject(StoneList srclist, long srcPos, long dstPos) {
    throw new SException("nyi");
  }

  public StoneList getList(StoneList.Long index) {
    throw new SException("nyi");
  }

  public StoneList getList(StoneList.Int index) {
    throw new SException("nyi");
  }

  public static class Dictionary extends StoneList {
    StoneList valList;
    StoneList keyList;

    public Dictionary() {
      super(sdb.engine.StoneLisp.Type.Dictionary.asByte(), 0);
      valList = new StoneList(0);
      keyList = new StoneList(0);
    }

    public long length() {
      return keyList.length();
    }

    public StoneList keys() {
      return keyList;
    }

    public StoneList values() {
      return valList;
    }

    public StoneObject getKeyObject(long n) {
      return keyList.getObject(n);
    }

    public StoneObject getValueObject(long n) {
      return valList.getObject(n);
    }

    public StoneObject getObject(long n) {
      if (n >= keyList.length())
        return StoneObject.Null;
      StoneList pair = new StoneList(2);  
      pair.putObject(0, keyList.getObject(n));
      pair.putObject(1, valList.getObject(n));
      sdb.engine.StoneLisp.get().register(pair); 
      return pair;
    }

    public long getPos(StoneObject key) {
      long pos = index.get(key); 
      if (index.exists(pos)) {
        pos = index.get_value(pos);
        return pos;
      }
      return -1;
    }

    public StoneObject getObject(StoneObject key) {
      long pos = index.get(key); 
      if (index.exists(pos)) {
        pos = index.get_value(pos);
        return valList.getObject(pos);
      }
      return StoneObject.Null;
    }

    public void putObject(long n, StoneObject obj) {
      throw new SException("not supported");
    }

    public void append(StoneList keys, StoneList vals) {
      long len  = keys.length();
      if (len != vals.length()) 
        throw new SException("length");
      for (long i = 0; i < len; i++) 
        upsert(keys.getObject(i), vals.getObject(i));
    }
    
    public void append(StoneObject key, StoneObject val) {
      upsert(key, val);
    }

    public void upsert(StoneObject key, StoneObject val) {
      long x = index.get(key); 
      if (index.exists(x)) { 
        valList.putObject(index.get_value(x), val);
      } else {
        keyList.append(key);
        index.put(keyList.length()-1); 
        valList.append(val);
      } 
    }

    public java.lang.String toString() {
      long len = length(); 
      StringBuffer buf = new StringBuffer(); 
      for (long i = 0; i < len; i++) {
        long x = index.get(keyList.getObject(i));
        buf.append(keyList.getObject(index.get_value(x)));
        buf.append("!");
        buf.append(valList.getObject(index.get_value(x)));
        buf.append(" ");
        buf.append("\n");
      }
      return buf.toString();
    }

    public void release() {
      if (decRefCount() > 0) 
        return;

      if (index != null) {
        index.release();
        index = null;
      }
      if (keyList != null) {
        keyList.release();
        keyList = null;
      }
      if (valList != null) {
        valList.release();
        valList = null;
      }
    }
  }

  public static class String extends StoneList {

    String(long address, byte m) {
      super(address, m);
    }

    public String(java.lang.String str) {
      this(str.toCharArray());
    }

    public String(char[] str) {
      super(StoneObject.Type.Char.asList(), str.length);
      for (int i = 0; i < str.length; i++) {
        mem.putChar(dataStart() + byteLength(i), str[i]);
      }
    }

    public String(StoneList.Byte obj, long length) {
      super(StoneObject.Type.Char.asList(), length, length);
      mem.copyMemory(obj.dataStart() + listOffset, dataStart(), length*2);       
    }
    
    private long byteLength(long n) {
      return n << 1;
    }

    public long getByteSize() {
      return listOffset + Type.getTypeSize(getType())*length();
    }

    public StoneObject first() {
      return StoneObject.alloc(getChar(0));
    }

    public char getChar(long pos) {
      if (pos >= length()) 
        return ' ';
      return mem.getChar(dataStart() + byteLength(pos));
    }

    public void putChar(long pos, char ch) {
      if (pos >= length()) 
        return;
      mem.putChar(dataStart() + byteLength(pos), ch);
    }

    public StoneObject getObject(long n) {
      throw new SException("nyi");
    }

    public void putObject(long n, StoneObject obj) {
      putChar(n, obj.getChar());
    }

    public boolean equals(long pos1, long pos2) {
      return getChar(pos1) == getChar(pos2);
    }

    public boolean equals(long pos, StoneObject obj) {
      return getChar(pos) == obj.getChar(); 
    }

    public static boolean equals(String str1, String str2) {
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
      if (obj instanceof String) 
        return equals(this, (String)obj);
      else if (obj instanceof StoneObject) {
        StoneObject so = (StoneObject)obj;
        if (so.getType() == StoneObject.Type.Char.asList())
          return equals(this, new String(so.address, so.getType()));
      }
      return false;
    }

    public int hashCode() {
      if (length() <=0) 
        return 0;
      long dataStart = dataStart();
      int h = mem.getChar(dataStart);
      for (long i = 1; i < length(); i++)
        h = (h << 5) - h + mem.getChar(dataStart + byteLength(i)); 
      return h;
    }

    public java.lang.String toString() {
      StringBuffer buf = new StringBuffer(); 
      long dataStart = dataStart();
      for (long i  = 0; i < length(); i++) {
        buf.append(mem.getChar(dataStart + byteLength(i)));
      }
      return buf.toString();
    }
    
  }

  public static class Lambda extends StoneList {
    StoneLisp.Lambda func;

    protected long byteOffset(long n) {
      return n << 3;
    }

    public Lambda(StoneList params, StoneList body) {
      super(StoneLisp.Type.Lambda.asByte(), 2);
      putObject(0, params);
      putObject(1, body);
    }

    public StoneObject first() {
      StoneObject so = getObject(0);
      so.retain();
      return so;
    }

    public StoneObject rest() {
      StoneList list = new StoneList(1);
      list.putObject(0, getObject(1));
      return list; 
    }

    public java.lang.String toString() {
      StringBuffer buf = new StringBuffer(); 
      buf.append("(lambda ");
      buf.append(getObject(0));
      buf.append(getObject(1));
      buf.append(")");
      return buf.toString();
    }

    private StoneList resolve(StoneLisp.Environment env, StoneList args) {
      StoneList params = (StoneList)args; 
      StoneList list = new StoneList(params.length());
      for (long i = 0; i < params.length(); i++) {
        StoneObject so = params.getObject(i);
        while (so instanceof StoneList.String) {
          so = env.get(so);
        }
        list.putObject(i, so);
      }

      params.release();
      return list;
    }

    public StoneObject eval(StoneLisp.Environment env, StoneList args) {
      StoneObject ret = StoneObject.Null;
      StoneObject params = resolve(env, (StoneList)args.rest());
      try {
        if (func == null) 
          func = new StoneLisp.Lambda(getObject(0), getObject(1));
        ret = func.eval(env, (StoneList)params);
      } catch (StoneLisp.LangException e) {
        env.debugError(e);
      }
      params.release();
      return ret;
    }    
  }

  public static class Symbol extends StoneList {
    public Symbol() {
      super(StoneObject.Type.Symbol.asList(), 0);
    }

    public Symbol(long size) {
      super(StoneObject.Type.Symbol.asList(), size, 0);
    }

    public Symbol(java.lang.String[] strs) {
      super(StoneObject.Type.Symbol.asList(), strs.length);
      init(strs);
    }

    private void init(java.lang.String[] strs) {
      for (int i = 0; i < strs.length; i++) 
        putObject(i, new StoneObject.Symbol(strs[i]));
    }
          
    public void appendList(StoneEnum list) {
      long inclen = list.length(); 
      if (inclen == 0) 
        return;
      long curlen = length();
      resize(curlen+inclen);
      for (long i=0; i < inclen; i++) 
        put(curlen+i, list.get(i));
    }

    public StoneObject.Symbol get(long pos) {
      return (StoneObject.Symbol)getObject(pos);
    }

    public void put(long pos, StoneObject.Symbol symbol) {
      putObject(pos, symbol);
    }

    public boolean equals(long pos1, long pos2) {
      return get(pos1).equals(get(pos2));
    }

    public boolean equals(long pos, StoneObject obj) {
      return get(pos).equals(obj);
    }

    public static boolean equals(Symbol syms1, Symbol syms2) {
      long len  = syms1.length(); 
      if (len != syms2.length()) 
        return false; 
      for (long pos = 0; pos < len; pos++) 
        if (!syms1.get(pos).equals(syms2.get(pos)))
          return false;
      return true;
    }
  }

  public static class Byte extends StoneList {
    private long base = 0;

    public Byte(long size) {
      super(StoneObject.Type.Byte.asList(), size, 0);
    }

    Byte(long address, byte m) {
      super(address, m);
    }

    public long getByteSize() {
      return listOffset + Type.getTypeSize(getType())*length();
    }

    public void seek(long num) {
      this.base += num;
    }

    public long getPos() {
      return this.base;
    }

    public void clear() {
      this.base = 0;
    }

    long dataStart() {
      return address + base + listOffset;
    }

    long remaining() {
      return length() - base;
    }

    private long byteLength(long n) {
      return n;
    }

    public void shuffle() {
      long len = length() - base;
      long pos = dataStart();
      base = 0;
      putLength(len);
      if (len > 0) 
        mem.copyMemory(pos, dataStart(), len);
    }

    public void append(StoneBuffer buf, int len) {
      long length = length();
      if ((length + len) > size())
        resizeBy(len);
      mem.copyMemory(buf.address(), dataStart()+length(), len);
      incLengthBy(len);
    }

    public void append(byte[] bytes) {
      long length = length();
      int len = bytes.length;
      if ((length + len) > size())
        resizeBy(len);
      for (int i = 0; i < len; i++) 
        put(length +i, bytes[i]);
      incLengthBy(len);
    }

    public byte get(long offset) {
      return peek(offset);
    }

    public byte get() {
      return peek(0);
    }

    public short getShort() {
      return getShort(0);
    }

    public long getLong() {
      return getLong(0);
    }

    public short getShort(long offset) {
      return mem.getShort(dataStart() + offset);
    }

    public int getInt(long offset) {
      return mem.getInt(dataStart() + offset);
    }

    public char getChar(long offset) {
      return mem.getChar(dataStart() + offset);
    }

    public long getLong(long offset) {
      return mem.getLong(dataStart() + offset);
    }

    public void put(long offset, byte b) {
      mem.putByte(dataStart() + offset, b);
    }

    public byte peek(long offset) {
      return mem.getByte(dataStart() + offset);
    }

    public boolean equals(long pos1, long pos2) {
      return get(pos1) == get(pos2);
    }

    public boolean equals(long pos, StoneObject obj) {
      return get(pos) == obj.getByte(); 
    }

    public java.lang.String toString() {
      StringBuilder sb = new StringBuilder();
      Formatter formatter = new Formatter(sb, Locale.US);
      
      for (long i = 0; i < remaining(); i++) 
        formatter.format("%x ", mem.getByte(dataStart()+i));
      return sb.toString();
    }
  }

  public static class Int extends StoneList {
    public Int(long size) {
      super(StoneObject.Type.Int.asList(), size);
    }

    public Int(long size, long length) {
      super(StoneObject.Type.Int.asList(), size, length);
    }

    Int(long address, byte m) {
      super(address, m);
    }
    
    public Int(StoneList.Byte obj, long length) {
      super(StoneObject.Type.Int.asList(), length, length);
      mem.copyMemory(obj.dataStart() + listOffset, dataStart(), length*4);
    }

    public long getByteSize() {
      return listOffset + Type.getTypeSize(getType())*length();
    }

    private long byteLength(long n) {
      return n << 2;
    }

    public StoneObject first() {
      return StoneObject.alloc(get(0));
    }

    public int get(long n) {
      checkLength(n);
      return mem.getInt(dataStart()+ byteLength(n));
    }

    public void putObject(long n, StoneObject obj) {
      put(n, obj.getInt());
    }

    public void add(long pos, StoneObject obj) {
      add(pos, obj.getInt());
    }

    public void addObject(StoneList srclist, long srcPos, long dstPos) {
      add(dstPos, ((Int)srclist).get(srcPos));
    }

    public void add(long n, int val) {
      checkLength(n);
      long pos = dataStart()+ byteLength(n);
      mem.putInt(pos, val + mem.getInt(pos));
    }

    public void put(long n, int val) {
      checkLength(n);     
      mem.putInt(dataStart() + byteLength(n), val);
    }

    public void append(StoneObject so) {
      append(so.getInt());
    }

    public void appendList(StoneList.Int list) {
      long inclen = list.length(); 
      if (inclen == 0) 
        return;
      long curlen = length();
      resize(curlen+inclen);
      for (long i=0; i < inclen; i++) 
        put(curlen+i, list.get(i));
    }

    public void appendList(StoneList.Long list) {
      long inclen = list.length(); 
      if (inclen == 0) 
        return;
      long curlen = length();
      resize(curlen+inclen);
      for (long i=0; i < inclen; i++) 
        put(curlen+i, (int)list.get(i));
    }

    public void appendList(StoneList.Double list) {
      long inclen = list.length(); 
      if (inclen == 0) 
        return;
      long curlen = length();
      resize(curlen+inclen);
      for (long i=0; i < inclen; i++) 
        put(curlen+i, (int)list.get(i));
    }

    public void append(int val) {
      long length = length();
      if (length >= size()) 
        realloc();
      incLength();
      put(length, val);
    }

    public void append(long val) {
      long length = length();
      if (length >= size()) 
        realloc();
      incLength();
      put(length, (int)val);
    }

    public void append(double val) {
      long length = length();
      if (length >= size()) 
        realloc();
      incLength();
      put(length, (int)val);
    }

    public boolean equals(long pos1, long pos2) {
      return get(pos1) == get(pos2);
    }

    public boolean equals(long pos, StoneObject obj) {
      return get(pos) == obj.getInt(); 
    }

    public static boolean equals(Int num1, Int num2) {
      long len = num1.length();
      if (len == num2.length()) {
        Listwhile(iter.hasNext()) {
        sum1 += iter.getDouble();
        iter.move();
      }
      return sum1 + sum2 + sum3 + sum4;
    }

    public int hashCode(long pos) {
      return StoneUtil.double2int(get(pos));
    }

    public int hashCode() {
      long len = length();

      len = len > PrintSize? PrintSize : len;
      int hash = StoneUtil.double2int(get(0));
      for (long i = 1; i < len; i++) 
        hash ^= StoneUtil.double2int(get(i));
      return hash;
    }

    public java.lang.String toString(long pos) {
      return new java.lang.Double(get(pos)).toString();
    }

    public java.lang.String toString() {
      StringBuffer buf = new StringBuffer();
      long len = length();

      len = len > PrintSize? PrintSize : len;
      buf.append("(");
      for (long i = 0; i < len; i++) {
        buf.append(get(i));
        buf.append(" ");
      }
      if (length() > PrintSize)
        buf.append(" ...");
      buf.append(")");
      return buf.toString();
    }

    public int getInt(long n) {
      return (int)get(n);
    }
    
    public long getLong(long n) {
      return (long)get(n);
    }

    public double getDouble(long n) {
      return get(n);
    }

    public StoneObject add(StoneObject obj) {
      StoneList.Double ret;
      long len;
      if (obj.isList()) {
        len = obj.length();
        if (len != length()) 
          throw new SException("length");
        ret = new StoneList.Double(len);
        StoneList l = (StoneList)obj;
        for (long i = 0; i < len; i++) 
          ret.put(i, getDouble(i) + l.getDouble(i));

      } else { 
        len = length();
        double n = obj.asDouble();
        ret = new StoneList.Double(length());
        for (long i = 0; i < len; i++) 
          ret.put(i, get(i) + n);
      }
      return ret;
    }

  }
}
