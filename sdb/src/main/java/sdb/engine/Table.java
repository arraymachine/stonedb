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

import org.apache.log4j.Logger;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.nio.MappedByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.LongBuffer;
import java.nio.IntBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.io.File;
import java.io.FileReader;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.IOException;
import java.text.ParseException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import org.joda.time.DateTime;
import org.joda.time.Instant;

public final class Table implements Sequential, ISeq, Counted, IObj {

  private static Logger logger = Logger.getLogger(Table.class.getName());
  private String[] col_names;
  private Column_Type[] col_types;
  private String[] col_names_clone; 
  private Column_Type[] col_types_clone;
  private Vector[] cols;
  private HashMap<String, Integer> col_map;
  int capacity;
  int cur;
  Table parent;
  HashIndex hashIndex;

  int limit = -1;

  public enum Column_Type {BOOLEAN, VARCHAR8, VARCHAR16, VARCHAR256, SYMBOL, TIMESTAMP, INT, DOUBLE, VECTOR, LONG};

  public static class TableException extends RuntimeException {
    public TableException(String msg) {
      super(msg);
    }
    public TableException(Exception e) {
      super(e);
    }
  }

  static class Rand {
    long x;

    public Rand() {
      this(0);
    }

    public Rand(long seed) {
      if (seed != 0)
        x = seed;
      else
        x = Instant.now().getMillis();
    }

    public long randomLong() {
      x ^= (x << 21); 
      x ^= (x >>> 35);
      x ^= (x << 4);
      return x;
    }

    public int nextInt() {
      return Math.abs((int)randomLong());
    }

    public long nextLong() {
      return Math.abs(randomLong());
    }
  }

  public static abstract class Vector {
    protected int cur = 0;
    Column_Type type;
    String fileName;
    boolean loaded = true;
    HashIndex hashIndex;
    
    public abstract long size();

    public Column_Type get_type() {
      return type;
    }

    public int length() {
      return cur;
    }

    public boolean is_indexed() {
      return hashIndex != null;
    }

    public HashIndex get_index() {
      return hashIndex;
    }
    
    public void clear_index() {
      hashIndex = null;
    }

    public void store_index(HashIndex hashIndex) {
      this.hashIndex = hashIndex;
    }
    
    public void copy(Vector src) {
      cur = src.cur;
    }

    public boolean isLoaded() {
      return loaded;
    }

    public abstract void append(Object data);

    public static Vector quick_load(String fileName)throws IOException {
      RandomAccessFile db = new RandomAccessFile(fileName, "r");
      FileChannel fc = null;
      try {
        fc = db.getChannel();
        MappedByteBuffer buf = fc.map(MapMode.READ_ONLY, 0, 8);
        int type = buf.getInt();
        int len = buf.getInt();

        if (type == Column_Type.TIMESTAMP.ordinal())
          return new TimestampVector(fileName, len);
        else if (type == Column_Type.DOUBLE.ordinal())
          return new DoubleVector(fileName, len);
        else if (type == Column_Type.INT.ordinal())
          return new IntVector(fileName, len);
        else if (type == Column_Type.LONG.ordinal())
          return new IntVector(fileName, len);
        else if (type == Column_Type.SYMBOL.ordinal())
          return new SymbolVector(fileName, len);

      } finally {
        if (fc != null)
          fc.close();
        db.close();
      }
      return null;
    }

    protected static int get_new_size(int old_size) {
      old_size += (old_size >> 3) + (old_size < 9 ? 3 : 6);
      return old_size;
    }

    Vector load() {
      try {
        if (!loaded && fileName != null) 
          return Vector.load(fileName);
      } catch (IOException e) {}
      throw new TableException("table data file not exist");
    }

    public static Vector load(String fileName) throws IOException {
      RandomAccessFile db = new RandomAccessFile(fileName, "r");
      FileChannel fc = null;
      try {
        fc = db.getChannel();
        MappedByteBuffer buf = fc.map(MapMode.READ_ONLY, 0, 4);
        int type = buf.getInt();
        int len = (int)fc.size() - 4;
        buf = fc.map(MapMode.READ_ONLY, 4, len);

        if (type == Column_Type.TIMESTAMP.ordinal())
          return TimestampVector.load(fileName, buf, len - 4);
        else if (type == Column_Type.DOUBLE.ordinal())
          return DoubleVector.load(fileName, buf, len - 4);
        else if (type == Column_Type.INT.ordinal())
          return IntVector.load(fileName, buf, len - 4);
        else if (type == Column_Type.LONG.ordinal())
          return IntVector.load(fileName, buf, len - 4);
        else if (type == Column_Type.SYMBOL.ordinal())
          return new SymbolVector(buf, len - 4, fileName);
        

      } finally {
        if (fc != null)
          fc.close();
        db.close();
      }
      return null;
    }

    public void append(double data) {
      throw new TableException("col type mismatch");
    }

    public void append(char[] data) {
      throw new TableException("col type mismatch");
    }

    public void append(long data) {
      throw new TableException("col type mismatch");
    }

    public void append(int data) {
      throw new TableException("col type mismatch");
    }

    public abstract Object get(int position);

    public Object first() {
      throw new TableException("nyi");
    }

    public Object last() {
      throw new TableException("nyi");
    }

    public void sample(int[] index, Object[] data) {
      throw new TableException("nyi");
    }

    public Vector get(IntVector index) {
      throw new TableException("col type mismatch");
    }

    public IntVector filter(Term.Operator op, Object value) {
      throw new TableException("nyi");
    }

    public Object sum(int start, int end) {
      throw new TableException("col type mismatch");
    }

    public Object sum(IntVector index) {
      throw new TableException("col type mismatch");
    }

    public Object avg(int start, int end) {
      throw new TableException("col type mismatch");
    }

    public Object avg(IntVector index) {
      throw new TableException("col type mismatch");
    }

    public Object mov_avg(IntVector index, int window) {
      throw new TableException("col type mismatch");
    }

    public Object sample(int window) {
      throw new TableException("col type mismatch");
    }

    public Object sample(IntVector index, int window) {
      throw new TableException("col type mismatch");
    }

    public Object mov_sum(IntVector index, int window) {
      throw new TableException("col type mismatch");
    }

    public Object min(IntVector index) {
      throw new TableException("col type mismatch");
    }

    public Object max(IntVector index) {
      throw new TableException("col type mismatch");
    }

    public Object min() {
      throw new TableException("col type mismatch");
    }

    public Object max() {
      throw new TableException("col type mismatch");
    }

    public Object plus(IntVector index, Object num) {
      throw new TableException("col type mismatch");
    }

    public Object minus(IntVector index, Object num) {
      throw new TableException("col type mismatch");
    }

    public Object plus(Object num) {
      throw new TableException("col type mismatch");
    }

    public Object minus(Object num) {
      throw new TableException("col type mismatch");
    }

    public int distinct_count(IntVector index) {
      throw new TableException("nyi");
    }

    public int distinct_count() {
      throw new TableException("nyi");
    }

    public void save(String fileName) throws IOException {
      throw new TableException("nyi");
    }

    public Object get_as_month(int position) {
      throw new TableException("nyi");
    }

    public Object get_as_day(int position) {
      throw new TableException("nyi");
    }

    public Object get_as_day_of_year(int position) {
      throw new TableException("nyi");
    }

    public Object get_as_year(int position) {
      throw new TableException("nyi");
    }

    public Object get_as_instant(int position) {
      throw new TableException("nyi");
    }

    public Vector get_as_month(IntVector index) {
      throw new TableException("nyi");
    }

    public Vector get_as_day(IntVector index) {
      throw new TableException("nyi");
    }

    public Vector get_as_day_of_year(IntVector index) {
      throw new TableException("nyi");
    }

    public Vector get_as_year(IntVector index) {
      throw new TableException("nyi");
    }

    public Vector get_as_instant(IntVector index) {
      throw new TableException("nyi");
    }

    public void rand(int size, int upper) {
      throw new TableException("nyi");
    }

    public void range(int size) {
      range(size, 1); 
    }

    public void range(int size, int step) {
      throw new TableException("nyi");
    }

    public boolean compare(Term.Operator op,  int pos, Object value) {
      throw new TableException("nyi");
    }

    public boolean equal(int pos, Object val) {
      throw new TableException("nyi");
    }

    public boolean notequal(int pos, Object val) {
      throw new TableException("nyi");
    }

    public boolean greater(int pos, Object val) {
      throw new TableException("nyi");
    }

    public boolean less(int pos, Object val) {
      throw new TableException("nyi");
    }

    public boolean greaterequal(int pos, Object val) {
      throw new TableException("nyi");
    }

    public boolean lessequal(int pos, Object val) {
      throw new TableException("nyi");
    }

  }

  public static final class CharVector extends Vector {

    CharBuffer data;
    int elm_size;

    public CharVector(int elm_size, int capacity) {
      data = CharBuffer.allocate(capacity*elm_size);
      this.elm_size = elm_size;
      switch (elm_size) {
        case 8:
          type = Column_Type.VARCHAR8;
          break;
        case 16:
          type = Column_Type.VARCHAR16;
          break;
        case 256:
          type = Column_Type.VARCHAR256;
          break;
        default:
          break;
      }
    }

    public CharVector(String fileName, long len, int elm_size) {
      this.fileName = fileName;
      loaded = false;
      cur = (int)len;;
      switch (elm_size) {
        case 8:
          type = Column_Type.VARCHAR8;
          break;
        case 16:
          type = Column_Type.VARCHAR16;
          break;
        case 256:
          type = Column_Type.VARCHAR256;
          break;
        default:
          break;
      }
    }

    public CharVector(CharBuffer buf, long len, int elm_size) throws IOException {
      data = CharBuffer.allocate((int)len);
      int nums = buf.read(data);
      this.elm_size = elm_size;
      cur = nums / elm_size;
      switch (elm_size) {
        case 8:
          type = Column_Type.VARCHAR8;
          break;
        case 16:
          type = Column_Type.VARCHAR16;
          break;
        case 256:
          type = Column_Type.VARCHAR256;
          break;
        default:
          break;
      }
    }

    public long size() {
      return 2*elm_size*(long)cur;
    }

    public void copy(Vector src) {
      super.copy(src);
      CharVector csrc = (CharVector)src;
      this.elm_size = csrc.elm_size;
      csrc.data.flip();
      data.put(csrc.data);
    }

    public void append(Object data) {
      char[] chars;
      int len;

      cur++;

      if (data instanceof CharBuffer) {
        len = ((CharBuffer)data).length();
        if (len > elm_size) {
          chars = ((CharBuffer)data).toString().toCharArray();
          this.data.put(chars, 0, elm_size);
          return;
        }
        this.data.put((CharBuffer)data);
      } else {
        chars = (char[])data;
        len = chars.length;
        if (len > elm_size) {
          this.data.put(chars, 0, elm_size);
          return;
        }
        this.data.put(chars, 0, chars.length);
      }

      for (int i = 0; i < (elm_size - len); i++)
        this.data.put(' ');
      clear_index();
    }

    public Object get(int position) {
      if (position >= cur)
        return "";
      int absPos = position * elm_size;
      int pos = data.position();
      if (absPos != pos)
        data.position(absPos);
      return data.subSequence(0, elm_size);
    }

    public void save(String fileName) throws IOException {
      RandomAccessFile file = new RandomAccessFile(fileName, "rw");
      FileChannel fc = null;
      try {
        fc = file.getChannel();
        MappedByteBuffer buf = fc.map(MapMode.READ_WRITE, 0, 4);
        switch (elm_size) {
          case 8:
            buf.putInt(Column_Type.VARCHAR8.ordinal()); 
            break;
          case 16:
            buf.putInt(Column_Type.VARCHAR16.ordinal()); 
            break;
          case 256:
            buf.putInt(Column_Type.VARCHAR256.ordinal());
            break;
          default:
            throw new TableException("nyi"); 
        }
        buf = fc.map(MapMode.READ_WRITE, 4, 4+size());
        data.flip();
        CharBuffer cbuf = buf.asCharBuffer();
        cbuf.put(data);
        buf.force();
      } finally {
        if (fc != null)
          fc.close();
        file.close();
      }
    }

  }

  public static final class BitVector extends Vector {
    boolean[] data;

    public BitVector(int capacity) {
      data = new boolean[capacity];
      type = Column_Type.BOOLEAN;
    }

    public long size() {
      return cur;
    }

    public void copy(Vector src) {
      super.copy(src);
      BitVector bsrc = (BitVector)src;
      System.arraycopy(bsrc.data, 0, data, 0, bsrc.data.length);
    }

    public void append(Object data) {
      this.data[cur++] = (Boolean)data;
      clear_index();
    }

    public Object get(int position) {
      if (position >= cur)
        return false;
      return data[position];
    }

    public Object sum(int start, int end) {
      int sum = 0;
      for (int i = start; i < end; i++)
        if (data[i])
          sum++;
      return sum;
    }

  }

  public static final class TimestampVector extends Vector {
    long[] data;

    TimestampVector() {}

    public TimestampVector(int capacity) {
      data = new long[capacity];
      type = Column_Type.TIMESTAMP;
    }

    public TimestampVector(String fileName, int len) {
      this.fileName = fileName;
      loaded = false;
      cur = len;
      type = Column_Type.TIMESTAMP;
    }

    public TimestampVector(LongBuffer buf, long len) {
      data = new long[(int)(len/8)];
      buf.get(data);
      cur = data.length;
      type = Column_Type.TIMESTAMP;
    }

    public long size() {
      return 8*(long)cur;
    }

    public void copy(Vector src) {
      super.copy(src);
      TimestampVector isrc = (TimestampVector)src;
      System.arraycopy(isrc.data, 0, data, 0, isrc.data.length);
    }

    private long tolong(Object data) {
      long val = 0;
      if (data instanceof Integer)
        val = ((Integer)data).intValue();
      else if (data instanceof DateTime)
        val = ((DateTime)data).getMillis();
      else 
        val = ((Long)data).longValue();
      return val;
    }

    public IntVector filter(Term.Operator op, Object value) {
      IntVector index = new IntVector(256);
      long arg = tolong(value);

      for (int i = 0; i < cur; i++) {
        boolean ret;
        switch(op) {
          case EQUAL:
            ret = Table.equal(data[i], arg);
            break;
          case GREATER:
            ret = Table.greater(data[i], arg);
            break;
          case GREATEREQUAL:
            ret = Table.greaterequal(data[i], arg);
            break;
          case LESS:
            ret = Table.less(data[i], arg);
            break;
          case LESSEQUAL:
            ret = Table.lessequal(data[i], arg);            
            break;
          case NOTEQUAL:
            ret = Table.notequal(data[i], arg);            
          default:
            throw new TableException("nyi");
        }
        if (ret)
          index.append(i);
      }
      return index;
    }

    public boolean compare(Term.Operator op, int pos, Object arg) {
      boolean ret;
      switch(op) {
        case EQUAL:
          ret = equal(pos, arg);
          break;
        case GREATER:
          ret = greater(pos, arg);
          break;
        case GREATEREQUAL:
          ret = greaterequal(pos, arg);
          break;
        case LESS:
          ret = less(pos, arg);
          break;
        case LESSEQUAL:
          ret = lessequal(pos, arg);            
          break;
        case NOTEQUAL:
          ret = notequal(pos, arg);            
        default:
          throw new TableException("nyi");
      }
      return ret;
    }

    public boolean equal(int pos, Object value) {
      return data[pos] == tolong(value);
    }

    public boolean notequal(int pos, Object value) {
      return data[pos] != tolong(value);
    }

    public boolean greater(int pos, Object value) {
      return data[pos] > tolong(value);
    }

    public boolean less(int pos, Object value) {
      return data[pos] < tolong(value);
    }

    public boolean greaterequal(int pos, Object value) {
      return data[pos] >= tolong(value);
    }

    public boolean lessequal(int pos, Object value) {
      return data[pos] <= tolong(value);
    }
    
    public void append(Object data) {
      append(tolong(data));
    }

    private void realloc() {
      long[] old_data = data;
      data = new long[get_new_size(data.length)];
      System.arraycopy(old_data, 0, data, 0, old_data.length);
    }

    public void append(long val) {
      if (cur >= data.length)
        realloc();
      this.data[cur++] = val;
      clear_index();
    }

    public void append(int data) {
      append((long)data);
    }

    public Object get(int position) {
      DateTime dt = new DateTime(data[position]);
      return dt;
    }

    public Vector get_as_month(IntVector iv) {
      int len = iv.length();

      IntVector tsv = new IntVector(iv.length()); 
      if (len <= 0)
        return tsv;

      for (int i = 0; i < len; i++) {
        DateTime dt = new DateTime(data[iv.fast_get(i)]);
        tsv.append(dt.getMonthOfYear()); 
      }
      return tsv;
    }

    public Vector get_as_day(IntVector iv) {
      int len = iv.length();

      IntVector tsv = new IntVector(iv.length()); 
      if (len <= 0)
        return tsv;

      for (int i = 0; i < len; i++) {
        DateTime dt = new DateTime(data[iv.fast_get(i)]);
        tsv.append(dt.getDayOfMonth()); 
      }
      return tsv;
    }

    public Vector get_as_day_of_year(IntVector iv) {
      int len = iv.length();

      IntVector tsv = new IntVector(iv.length()); 
      if (len <= 0)
        return tsv;

      for (int i = 0; i < len; i++) {
        DateTime dt = new DateTime(data[iv.fast_get(i)]);
        tsv.append(dt.getDayOfYear()); 
      }
      return tsv;
    }

    public Vector get_as_year(IntVector iv) {
      int len = iv.length();

      IntVector tsv = new IntVector(iv.length()); 
      if (len <= 0)
        return tsv;

      for (int i = 0; i < len; i++) {
        DateTime dt = new DateTime(data[iv.fast_get(i)]);
        tsv.append(dt.getYear()); 
      }
      return tsv;
    }

    public Vector get_as_instant(IntVector iv) {
      int len = iv.length();

      LongVector tsv = new LongVector(iv.length()); 
      if (len <= 0)
        return tsv;

      for (int i = 0; i < len; i++) 
        tsv.append(data[iv.fast_get(i)]); 

      return tsv;
    }

    public Vector get(IntVector iv) {
      int len = iv.length();

      TimestampVector tsv = new TimestampVector(iv.length()); 
      if (len <= 0)
        return tsv;

      for (int i = 0; i < len; i++) {
        tsv.append(data[iv.fast_get(i)]); 
      }
      return tsv;
    }

    public void sample(int[] index, Object[] data) {
      int len = 0;
      if (index == null || data == null || (len = index.length) != data.length)
        throw new TableException("length mismatch");

      for (int i = 0; i < len; i++) {
        data[i] = get(index[i]); 
      }
    }

    private long internal_get(int position) {
      if (position >= cur)
        return 0;
      return data[position];
    }

    public int fast_get_as_month(int position) {
      DateTime dt = new DateTime(data[position]);
      return dt.getMonthOfYear();
    }

    public Object get_as_month(int position) {
      DateTime dt = new DateTime(data[position]);
      return dt.getMonthOfYear();
    }

    public int fast_get_as_day(int position) {
      DateTime dt = new DateTime(data[position]);
      return dt.getDayOfMonth();
    }

    public Object get_as_day(int position) {
      DateTime dt = new DateTime(data[position]);
      return dt.getDayOfMonth();
    }

    public Object get_as_day_of_year(int position) {
      DateTime dt = new DateTime(data[position]);
      return dt.getDayOfYear();
    }

    public int fast_get_as_day_of_year(int position) {
      DateTime dt = new DateTime(data[position]);
      return dt.getDayOfYear();
    }

    public int fast_get_as_year(int position) {
      DateTime dt = new DateTime(data[position]);
      return dt.getYear();
    }

    public Object get_as_year(int position) {
      DateTime dt = new DateTime(data[position]);
      return dt.getYear();
    }

    public long fast_get_as_instant(int position) {
      return data[position];
    }

    public Object get_as_instant(int position) {
      return fast_get_as_instant(position);
    }

    public void rand(int size, int upper) {
      long now = Instant.now().getMillis();
      data = new long[size];
      Rand rand = new Rand();
      for (int i = 0; i < size; i++) {
        int next = rand.nextInt()%upper;
        data[i] = now + next ;
      }
      cur = size;
    }

    public void range(int size, int step) {
      if (step < 1)
        step = 1;
      long now = Instant.now().getMillis() - step;
      data = new long[size];
      for(int i = 0; i < size; i++) {
        now += step;
        data[i] = now;
      }
      cur = size;
    }

    static TimestampVector load(String fileName, MappedByteBuffer buf, int size) throws IOException {
      int count = size / 8;
      int totalSize = buf.getInt();
      long[] data = new long[totalSize];
      buf.asLongBuffer().get(data, 0, count);
      int id = 0;
      while (count < totalSize) {
        RandomAccessFile file = new RandomAccessFile(fileName+"__"+id, "r");
        FileChannel fc = file.getChannel();
        int len = (int)fc.size();
        buf = fc.map(MapMode.READ_ONLY, 0, len);
        len = len/8;
        long[] tempData = new long[len];
        buf.asLongBuffer().get(tempData, 0, len);
        System.arraycopy(tempData, 0, data, count, len);
        fc.close();
        file.close();
        count += len;
        id++;
      }
      TimestampVector v = new TimestampVector();
      v.cur = totalSize;
      v.type = Column_Type.TIMESTAMP;
      v.data = data;
      return v;
    }

    public void save(String fileName) throws IOException {
      RandomAccessFile file = new RandomAccessFile(fileName, "rw");
      FileChannel fc = file.getChannel();
      MappedByteBuffer buf = fc.map(MapMode.READ_WRITE, 0, 8);
      buf.putInt(type.ordinal());
      buf.putInt(cur);

      int count = cur;
      int threshold = 200000000;
      long start = 8;
      long len = 0;
      int pos = 0, rc = 0;

      if (count > threshold) {
        len = threshold*8;
        count -= threshold;
        rc = threshold;
      } else {
        rc = count;
        len = count*8;
        count = 0;
      }
      buf = fc.map(MapMode.READ_WRITE, start, len);
      LongBuffer lbuf = buf.asLongBuffer();
      lbuf.put(data, pos, rc);
      buf.force();
      fc.close();
      file.close();

      int id = 0;
      while (count > 0) {
        if (count > threshold) {
          len = threshold*8;
          count -= threshold;
          rc = threshold;
        }
        else {
          rc = count;
          len = count*8;
          count = 0;
        }
        file = new RandomAccessFile(fileName+"__"+id, "rw");
        fc = file.getChannel();
        buf = fc.map(MapMode.READ_WRITE, 0, len);
        lbuf = buf.asLongBuffer();
        lbuf.put(data, pos, rc);
        buf.force();
        fc.close();
        file.close();
        pos = rc;
        id++;
      }
    }

    public String toString() {
      StringBuffer buf = new StringBuffer();
      DateTime dt = new DateTime();
      buf.append('[');
      for (int i = 0; i < cur; i++) {
        buf.append(dt.withMillis(data[i]));
        buf.append(' ');
      }
      buf.append(']');
      return buf.toString();
    }
  }


  public static final class SymbolVector extends Vector {
    SymbolIndex index;
    char[][] data;
    IntVector cache_ids;

    static final class SymbolIndex {
      Dictionary<char[], Integer> dict;
      boolean is_new;
      
      public SymbolIndex() {
        dict = new Dictionary(false,
                              new Dictionary.CharArrayHashFunction(),
                              new Dictionary.CharArrayHashEqualFunction(), 16); 
      }

      public SymbolIndex(int capacity) {
        dict = new Dictionary(false,
                              new Dictionary.CharArrayHashFunction(),
                              new Dictionary.CharArrayHashEqualFunction(), capacity); 
      }

      public int map(char[] value) {
        is_new = false;
        int size = dict.size();
        int x = dict.put(value);
        if (dict.size() > size)
          is_new = true;
        return x;
      }

      public int lookup(char[] value) {
        int x = dict.get(value);
        return x;
      }

      public char[] map(int id) {
        return dict.get_key(id);
      }

      public boolean exists(int id) {
        return dict.exists(id);
      }

      public String toString() {
        return dict.toString();
      }
    }

    public SymbolVector() {
      index = new SymbolIndex();
      data = new char[0][];
      type = Column_Type.SYMBOL;
    }

    public SymbolVector(String[] syms) {
      index = new SymbolIndex();
      for (String str : syms) {
        if (str.length()>0)
          index.map(str.toCharArray());
      }
      data = new char[0][];
      type = Column_Type.SYMBOL;
    }

    public SymbolVector(SymbolVector sv, int capacity) {
      index = new SymbolIndex(capacity);
      data = new char[0][];
      type = Column_Type.SYMBOL;
    }

    public SymbolVector(String fileName, int len) {
      this.fileName = fileName;
      loaded = false;
      cur = len;
      type = Column_Type.SYMBOL;
    }

    public SymbolVector(int capacity) {
      index = new SymbolIndex(capacity);
      data = new char[capacity][];
      type = Column_Type.SYMBOL;
    }


    public SymbolVector(MappedByteBuffer idsbuf, int len, String fileName) {
      IntVector ids = null;
      try {
        ids = IntVector.load(fileName, idsbuf, len);

        RandomAccessFile file = new RandomAccessFile(fileName + ".sym", "r");
        FileChannel fc = file.getChannel();
        MappedByteBuffer buf = fc.map(MapMode.READ_ONLY, 0, fc.size());
        index = new SymbolIndex();
        index.dict.n_buckets = buf.getInt();
        int[] flags = new int[Dictionary.flag_size(index.dict.n_buckets)];
        index.dict.keys = new char[index.dict.n_buckets][];
        index.dict.flags = flags;
        Dictionary.clear_flags(flags);
        int count = (int)fc.size() - 4;
        int size = 0;
        while (count > 0) {
          int x = buf.getInt();
          len = buf.getInt();
          char[] sym = new char[len];
          for (int i = 0; i < len; i++)
            sym[i] = buf.getChar();
          index.dict.keys[x] = sym;
          Dictionary.clear_both(flags, x);
          size++;
          count -= 2 * 4 + 2*len;
        }
        index.dict.size = size;
        buf.force();
        fc.close();
        file.close();
      } catch(IOException e) {
        throw new TableException(e);
      }
      if (ids == null) 
        throw new TableException("no data");
      cur = ids.length();
      data = new char[cur][];
      for (int i = 0; i < cur; i++) {
        data[i] = index.map(ids.data[i]);
      }

      type = Column_Type.SYMBOL;
    } 


    public void sample(int[] index, Object[] data) {
      int len = 0;
      if (index == null || data == null || (len = index.length) != data.length)
        throw new TableException("length mismatch");

      for (int i = 0; i < len; i++) {
        data[i] = new String((char[])get(index[i])); 
      }
    }

    public Object first() {
      return new String((char[])get(0));
    }

    public Object last() {
      return new String((char[])get(cur-1)); 
    }


    public int distinct_count(IntVector index) {
      int len = index.length();

      if (len <= 0)
        return 0;

      Dictionary<char[], Integer> dict = new Dictionary(false,
        new Dictionary.CharArrayHashFunction(),
        new Dictionary.CharArrayHashEqualFunction(), 2);  
      for (int i = 0; i < len; i++)  
        dict.put((char[])get(i));
      return dict.size();
    }

    public int distinct_count() {
      return index.dict.size();
    }

    public void rand(int size, int upper) {
      data = new char[size][];
      cur = size;

      Rand rand = new Rand();

      int s = index.dict.end();
      int count = 0;
      while (size > 0) {
        int x = rand.nextInt() % s;
        if (index.exists(x)) {
          data[count++] = index.map(x);
          size--;
        }
      }
    }

    public void range(int size, int step) {
      if (step < 1)
        step = 1;
      data = new char[size][];
      cur = size;

      int s = index.dict.size();
      int count = size;

      while (count>0) {
        for (int i = 0; i < size; i++) {
          int x = (i*step) % s;
          if (index.exists(x)) {
            data[i] = index.map(x);
            count--;
          }
        }
      }
    }

    public long size() {
      return 4*(long)cur;
    }

    public void copy(Vector src) {
      super.copy(src);
      SymbolVector ssrc = (SymbolVector)src;
      this.data = ssrc.data;
      this.index = ssrc.index;
    }

    public int fast_get(int position) {
      if (cache_ids == null)
        cache_ids = build_ids();
      return cache_ids.fast_get(position);
    }

    public int fast_get(String data) {
      return index.lookup(data.toCharArray());
    }

    public Object get(int position) {
      return data[position]; 
    }

    public Vector get(IntVector index) {
      int len = index.length();
      SymbolVector sv = new SymbolVector(this, index.length()); 
      if (len <= 0)
        return sv;

      for (int i = 0; i < len; i++) {
        sv.append(get(index.fast_get(i))); 
      }

      sv.cur = len;
      return sv;
    }

    public void append(Object data) {
      if (data instanceof char[]) 
        append((char[])data);
      else if (data instanceof String) 
        append(((String)data).toCharArray());
      else 
        throw new TableException("nyi: " + data);
    }

    public IntVector filter(Term.Operator op, Object value) {
      IntVector iv = new IntVector(256);
      int id = toid(value);
      for (int i = 0; i < cur; i++) {
        int pos = fast_get(i);
        boolean ret;
        switch(op) {
          case EQUAL:
            ret = Table.equal(pos, id);
            break;
          case NOTEQUAL:
            ret = Table.notequal(pos, id);
            break;
          default:
            throw new TableException("nyi");
        }
        if (ret) {
          iv.append(i);
        }
      }
      return iv;
    }

    public boolean compare(Term.Operator op, int pos, Object value) {
      boolean ret;
      switch(op) {
        case EQUAL:
          ret = equal(pos, value);
          break;
        case NOTEQUAL:
          ret = notequal(pos, value);            
        default:
          throw new TableException("nyi");
      }
      return ret;
    }

    private int toid(Object data) {
      int val = -1;
      if (data instanceof char[])
        val = index.lookup((char[])data);
      else if (data instanceof String)
        val = index.lookup(((String)data).toCharArray());
      else 
        throw new TableException("data type");
      return val;
    }

    public boolean equal(int pos, Object val) {
      return fast_get(pos) == toid(val);
    }

    public boolean notequal(int pos, Object val) {
      return fast_get(pos) != toid(val);
    }

    private void append_data(char[] sym) {
      if (cur >= data.length) {
        char[][] old_data = data;
        data = new char[get_new_size(data.length)][];
        System.arraycopy(old_data, 0, data, 0, old_data.length);
      }
      data[cur++] = sym;
    }
    
    public void append(char[] data) {
      int id = index.map(data);
      if (index.is_new) 
        cache_ids = null;
      else if (cache_ids != null)
          cache_ids.append(id);
      append_data(index.map(id));
      clear_index();
    }

    private IntVector build_ids() {
      IntVector ids = new IntVector(data.length);
      for (int i = 0; i < cur; i++) 
        ids.append(index.lookup(data[i]));

      return ids;
    }
    
    public void save(String fileName) throws IOException {
      if (cache_ids == null)
        cache_ids = build_ids();
      cache_ids.save(Column_Type.SYMBOL.ordinal(), fileName);

      int len = 4;
      for (int i = 0; i < index.dict.end(); i++) {
        if (index.dict.exists(i)) {
          len += 2*4 + 2* index.dict.get_key(i).length;
        }
      }
      RandomAccessFile file = new RandomAccessFile(fileName + ".sym", "rw");
      FileChannel fc = file.getChannel();
      MappedByteBuffer buf = fc.map(MapMode.READ_WRITE, 0, len);
      buf.putInt(index.dict.end());

      for (int i = 0; i < index.dict.end(); i++) {
        if (index.dict.exists(i)) {
          buf.putInt(i);
          char[] sym = index.dict.get_key(i);
          buf.putInt(sym.length);
          for (int j = 0; j < sym.length; j++)
            buf.putChar(sym[j]);
        }
      }
      buf.force();
      fc.close();
      file.close();
    }

    public String toString() {
      StringBuffer buf = new StringBuffer();
      buf.append('[');
      for (int i = 0; i < cur; i++) {
        buf.append('\"');
        buf.append(new String((char[])get(i)));
        buf.append('\"');
        if (i != cur -1)
          buf.append(',');
      }
      buf.append(']');
      return buf.toString();
    }

  }

  public static final class IntVector extends Vector {
    int[] data;

    public IntVector() {}

    public IntVector(int capacity) {
      data = new int[capacity];
      type = Column_Type.INT;
    }

    public IntVector(int[] ints) {
      data = new int[ints.length];
      System.arraycopy(ints, 0, data, 0, ints.length);
      cur = data.length;
      type = Column_Type.INT;
    }

    public IntVector(String fileName, int len) {
      this.fileName = fileName;
      loaded = false;
      cur = len;
      type = Column_Type.INT;
    }

    public IntVector(IntBuffer buf, long len) {
      data = new int[(int)(len/4)];
      buf.get(data);
      cur = data.length;
      type = Column_Type.INT;
    }

    public long size() {
      return 4*(long)cur;
    }

    public void copy(Vector src) {
      super.copy(src);
      IntVector isrc = (IntVector)src;
      System.arraycopy(isrc.data, 0, data, 0, isrc.data.length);
    }

    private void realloc() {
      int[] old_data = data;
      data = new int[get_new_size(data.length)];
      System.arraycopy(old_data, 0, data, 0, old_data.length);
    }

    private int toInt(Object num) {
      int val = 0;
      if (num instanceof Double)
        val = ((Double)num).intValue();
      else if (num instanceof Integer)
        val = (Integer)num;
      else if (num instanceof Long)
        val = ((Long)num).intValue();
      else 
        throw new TableException("data type");
      return val;
    }

    public IntVector filter(Term.Operator op, Object value) {
      IntVector index = new IntVector(256);
      int arg = toInt(value);

      for (int i = 0; i < cur; i++) {
        boolean ret;
        switch(op) {
          case EQUAL:
            ret = Table.equal(data[i], arg);
            break;
          case GREATER:
            ret = Table.greater(data[i], arg);
            break;
          case GREATEREQUAL:
            ret = Table.greaterequal(data[i], arg);
            break;
          case LESS:
            ret = Table.less(data[i], arg);
            break;
          case LESSEQUAL:
            ret = Table.lessequal(data[i], arg);            
            break;
          case NOTEQUAL:
            ret = Table.notequal(data[i], arg);            
          default:
            throw new TableException("nyi");
        }
        if (ret)
          index.append(i);
      }
      return index;
    }

    public boolean compare(Term.Operator op, int pos, Object arg) {
      boolean ret;
      switch(op) {
        case EQUAL:
          ret = equal(pos, arg);
          break;
        case GREATER:
          ret = greater(pos, arg);
          break;
        case GREATEREQUAL:
          ret = greaterequal(pos, arg);
          break;
        case LESS:
          ret = less(pos, arg);
          break;
        case LESSEQUAL:
          ret = lessequal(pos, arg);            
          break;
        case NOTEQUAL:
          ret = notequal(pos, arg);            
        default:
          throw new TableException("nyi");
      }
      return ret;
    }

    public boolean equal(int pos, Object value) {
      return data[pos] == toInt(value);
    }

    public boolean notequal(int pos, Object value) {
      return data[pos] != toInt(value);
    }

    public boolean greater(int pos, Object value) {
      return data[pos] > toInt(value);
    }

    public boolean less(int pos, Object value) {
      return data[pos] < toInt(value);
    }

    public boolean greaterequal(int pos, Object value) {
      return data[pos] >= toInt(value);
    }

    public boolean lessequal(int pos, Object value) {
      return data[pos] <= toInt(value);
    }
    
    public void append(int num) {
      if (cur >= data.length)
        realloc();
      this.data[cur++] = num;
      clear_index();
    }

    public void append(Object data) {
      append(((Integer)data).intValue());
    }

    public int fast_get(int position) {
      if (position >= cur || position<0)
        return 0;
      return data[position];
    }

    public int fast_first() {
      return fast_get(0);
    }

    public int fast_last() {
      return fast_get(cur -1);
    }

    public int distinct_count(IntVector index) {
      int len = index.length();
      if (len <= 0)
        return 0;

      Dictionary<Integer, Integer> dict = new Dictionary(false, new Dictionary.IntHashFunction(), new Dictionary.IntHashEqualFunction(), 2);
      for (int i = 0; i < len; i++) {
        dict.put(data[index.fast_get(i)]);
      }
      return dict.size();
    }

    public int distinct_count() {
      Dictionary<Integer, Integer> dict = new Dictionary(false, new Dictionary.IntHashFunction(), new Dictionary.IntHashEqualFunction(), 2);
      for (int i = 0; i < cur; i++) 
        dict.put(data[i]);
      return dict.size();
    }

    public Object get(int position) {
      return fast_get(position);
    }

    public Vector get(IntVector index) {
      int len = index.length();
      IntVector dv = new IntVector(index.length());
      if (len <= 0)
        return dv;

      for (int i = 0; i < len; i++) {
        dv.append(data[index.fast_get(i)]);
      }
      return dv;
    }

    public void sample(int[] index, Object[] data) {
      int len = 0;
      if (index == null || data == null || (len = index.length) != data.length)
        throw new TableException("length mismatch");

      for (int i = 0; i < len; i++) {
        data[i] = get(index[i]); 
      }
    }

    public Object first() {
      return fast_get(0);
    }

    public Object last() {
      if (cur > 0)
        return fast_get(cur -1);
      return 0;
    }

    public void rand(int size, int upper) {
      data = new int[size];
      cur = size;

      Rand rand = new Rand();

      for (int i = 0; i < size; i++)
        data[i] = rand.nextInt() % upper;

    }

    public void range(int size, int step) {
      if (step < 1) 
        step = 1;
      data = new int[size];
      cur = size;

      for (int i = 0; i < size; i++)
        data[i] = i*step;

    }

    public Object sum(int start, int end) {
      double sum = 0;
      for (int i = start; i <= end; i++)
        sum += data[i];
      return BigInteger.valueOf((long)sum);
    }

    public Object avg(int start, int end) {
      double sum = 0;
      for (int i = start; i <= end; i++)
        sum += data[i];

      return sum/(end - start);
    }

    public Object sum(IntVector index) {
      double sum = 0;
      for (int i = 0; i < index.cur; i++)
        sum += data[index.data[i]];
      return BigInteger.valueOf((long)sum);
    }

    public Object avg(IntVector index) {
      double sum = 0;
      for (int i = 0; i < index.cur; i++)
        sum += data[index.data[i]];
      return sum/index.cur; 
    }

    public void save(String fileName) throws IOException {
      save(Column_Type.INT.ordinal(), fileName);
    }

    static final int threshold = 400000000;

    static IntVector load(String fileName, MappedByteBuffer buf, int size) throws IOException {
      int count = size / 4;
      int totalSize = buf.getInt();
      int[] data = new int[totalSize];
      buf.asIntBuffer().get(data, 0, count);
      int id = 0;
      while (count < totalSize) {
        RandomAccessFile file = new RandomAccessFile(fileName+"__"+id, "r");
        FileChannel fc = file.getChannel();
        int len = (int)fc.size();
        buf = fc.map(MapMode.READ_ONLY, 0, len);
        buf.asIntBuffer().get(data, count, len/4);
        fc.close();
        file.close();
        count += len/4;
        id++;
      }
      IntVector v = new IntVector();
      v.cur = totalSize;
      v.type = Column_Type.INT;
      v.data = data;
      return v;
    }

    public void save(int type, String fileName) throws IOException {
      RandomAccessFile file = new RandomAccessFile(fileName, "rw");
      FileChannel fc = file.getChannel();
      MappedByteBuffer buf = fc.map(MapMode.READ_WRITE, 0, 8);
      buf.putInt(type);
      buf.putInt(cur);

      int count = cur;
      long start = 8;
      long len = 0;
      int pos = 0, rc = 0;

      if (count > threshold) {
        len = threshold*4;
        count -= threshold;
        rc = threshold;
      } else {
        rc = count;
        len = count*4;
        count = 0;
      }
      buf = fc.map(MapMode.READ_WRITE, start, len);
      IntBuffer ibuf = buf.asIntBuffer();
      ibuf.put(data, pos, rc);
      buf.force();
      fc.close();
      file.close();

      int id = 0;
      while (count > 0) {
        if (count > threshold) {
          len = threshold*4;
          count -= threshold;
          rc = threshold;
        }
        else {
          rc = count;
          len = count*4;
          count = 0;
        }
        file = new RandomAccessFile(fileName+"__"+id, "rw");
        fc = file.getChannel();
        buf = fc.map(MapMode.READ_WRITE, 0, len);
        ibuf = buf.asIntBuffer();
        ibuf.put(data, pos, rc);
        buf.force();
        fc.close();
        file.close();
        pos = rc;
        id++;
      }
    }

    public String toString() {
      StringBuffer buf = new StringBuffer();
      buf.append('[');
      for (int i = 0; i < cur; i++) {
        buf.append(data[i]);
        if (i != cur -1)
          buf.append(',');
      }
      buf.append(']');
      return buf.toString();
    }

  }

  public static final class LongVector extends Vector {
    long[] data;

    public LongVector() {}

    public LongVector(int capacity) {
      data = new long[capacity];
      type = Column_Type.LONG;
    }

    public LongVector(long[] longs) {
      data = new long[longs.length];
      System.arraycopy(longs, 0, data, 0, longs.length);
      cur = data.length;
      type = Column_Type.LONG;
    }

    public LongVector(String fileName, int len) {
      this.fileName = fileName;
      loaded = false;
      cur = len;
      type = Column_Type.LONG;
    }

    public LongVector(LongBuffer buf, long len) {
      data = new long[(int)(len/4)];
      buf.get(data);
      cur = data.length;
      type = Column_Type.LONG;
    }

    public long size() {
      return 4*(long)cur;
    }

    public void copy(Vector src) {
      super.copy(src);
      LongVector isrc = (LongVector)src;
      System.arraycopy(isrc.data, 0, data, 0, isrc.data.length);
    }

    private void realloc() {
      long[] old_data = data;
      data = new long[get_new_size(data.length)];
      System.arraycopy(old_data, 0, data, 0, old_data.length);
    }

    private long toLong(Object num) {
      long val = 0;
      if (num instanceof Double)
        val = ((Double)num).longValue();
      else if (num instanceof Integer)
        val = ((Integer)num).longValue();
      else if (num instanceof Long)
        val = (Long)num;
      else 
        throw new TableException("data type");
      return val;
    }

    public IntVector filter(Term.Operator op, Object value) {
      IntVector index = new IntVector(256);
      long arg = toLong(value);

      for (int i = 0; i < cur; i++) {
        boolean ret;
        switch(op) {
          case EQUAL:
            ret = Table.equal(data[i], arg);
            break;
          case GREATER:
            ret = Table.greater(data[i], arg);
            break;
          case GREATEREQUAL:
            ret = Table.greaterequal(data[i], arg);
            break;
          case LESS:
            ret = Table.less(data[i], arg);
            break;
          case LESSEQUAL:
            ret = Table.lessequal(data[i], arg);            
            break;
          case NOTEQUAL:
            ret = Table.notequal(data[i], arg);            
          default:
            throw new TableException("nyi");
        }
        if (ret)
          index.append(i);
      }
      return index;
    }

    public boolean compare(Term.Operator op, int pos, Object arg) {
      boolean ret;
      switch(op) {
        case EQUAL:
          ret = equal(pos, arg);
          break;
        case GREATER:
          ret = greater(pos, arg);
          break;
        case GREATEREQUAL:
          ret = greaterequal(pos, arg);
          break;
        case LESS:
          ret = less(pos, arg);
          break;
        case LESSEQUAL:
          ret = lessequal(pos, arg);            
          break;
        case NOTEQUAL:
          ret = notequal(pos, arg);            
        default:
          throw new TableException("nyi");
      }
      return ret;
    }

    public boolean equal(int pos, Object value) {
      return data[pos] == toLong(value);
    }

    public boolean notequal(int pos, Object value) {
      return data[pos] != toLong(value);
    }

    public boolean greater(int pos, Object value) {
      return data[pos] > toLong(value);
    }

    public boolean less(int pos, Object value) {
      return data[pos] < toLong(value);
    }

    public boolean greaterequal(int pos, Object value) {
      return data[pos] >= toLong(value);
    }

    public boolean lessequal(int pos, Object value) {
      return data[pos] <= toLong(value);
    }
    
    public void append(long num) {
      if (cur >= data.length)
        realloc();
      this.data[cur++] = num;
      clear_index();
    }

    public void append(Object data) {
      if (data instanceof BigInteger)
        append(((BigInteger)data).longValue());
      else
        append(((Long)data).longValue());
    }

    public long fast_get(int position) {
      if (position >= cur || position<0)
        return 0;
      return data[position];
    }

    public long fast_first() {
      return fast_get(0);
    }

    public long fast_last() {
      return fast_get(cur -1);
    }

    public Object get(int position) {
      return fast_get(position);
    }

    public Vector get(IntVector index) {
      int len = index.length();
      LongVector dv = new LongVector(index.length());
      if (len <= 0)
        return dv;

      for (int i = 0; i < len; i++) {
        dv.append(data[index.fast_get(i)]);
      }
      return dv;
    }

    public void sample(int[] index, Object[] data) {
      int len = 0;
      if (index == null || data == null || (len = index.length) != data.length)
        throw new TableException("length mismatch");

      for (int i = 0; i < len; i++) {
        data[i] = get(index[i]); 
      }
    }

    public Object first() {
      return fast_get(0);
    }

    public Object last() {
      if (cur > 0)
        return fast_get(cur -1);
      return 0;
    }

    public void rand(int size, int upper) {
      data = new long[size];
      cur = size;

      Rand rand = new Rand();

      for (int i = 0; i < size; i++)
        data[i] = rand.nextLong() % upper;

    }

    public void range(int size, int step) {
      if (step < 1) 
        step = 1;
      data = new long[size];
      cur = size;

      for (int i = 0; i < size; i++)
        data[i] = i*step;

    }

    public Object sum(int start, int end) {
      double sum = 0;
      for (int i = start; i <= end; i++)
        sum += data[i];
      return BigInteger.valueOf((long)sum);
    }

    public Object avg(int start, int end) {
      double sum = 0;
      for (int i = start; i <= end; i++)
        sum += data[i];

      return sum/(end - start);
    }

    public Object sum(IntVector index) {
      double sum = 0;
      for (int i = 0; i < index.cur; i++)
        sum += data[index.data[i]];
      return BigInteger.valueOf((long)sum);
    }

    public Object avg(IntVector index) {
      double sum = 0;
      for (int i = 0; i < index.cur; i++)
        sum += data[index.data[i]];
      return sum/index.cur; 
    }

    public void save(String fileName) throws IOException {
      save(Column_Type.LONG.ordinal(), fileName);
    }

    static final int threshold = 400000000;

    static LongVector load(String fileName, MappedByteBuffer buf, int size) throws IOException {
      int count = size / 4;
      int totalSize = buf.getInt();
      long[] data = new long[totalSize];
      buf.asLongBuffer().get(data, 0, count);
      int id = 0;
      while (count < totalSize) {
        RandomAccessFile file = new RandomAccessFile(fileName+"__"+id, "r");
        FileChannel fc = file.getChannel();
        int len = (int)fc.size();
        buf = fc.map(MapMode.READ_ONLY, 0, len);
        buf.asLongBuffer().get(data, count, len/4);
        fc.close();
        file.close();
        count += len/4;
        id++;
      }
      LongVector v = new LongVector();
      v.cur = totalSize;
      v.type = Column_Type.LONG;
      v.data = data;
      return v;
    }

    public void save(int type, String fileName) throws IOException {
      RandomAccessFile file = new RandomAccessFile(fileName, "rw");
      FileChannel fc = file.getChannel();
      MappedByteBuffer buf = fc.map(MapMode.READ_WRITE, 0, 8);
      buf.putInt(type);
      buf.putInt(cur);

      int count = cur;
      long start = 8;
      long len = 0;
      int pos = 0, rc = 0;

      if (count > threshold) {
        len = threshold*4;
        count -= threshold;
        rc = threshold;
      } else {
        rc = count;
        len = count*4;
        count = 0;
      }
      buf = fc.map(MapMode.READ_WRITE, start, len);
      LongBuffer ibuf = buf.asLongBuffer();
      ibuf.put(data, pos, rc);
      buf.force();
      fc.close();
      file.close();

      int id = 0;
      while (count > 0) {
        if (count > threshold) {
          len = threshold*4;
          count -= threshold;
          rc = threshold;
        }
        else {
          rc = count;
          len = count*4;
          count = 0;
        }
        file = new RandomAccessFile(fileName+"__"+id, "rw");
        fc = file.getChannel();
        buf = fc.map(MapMode.READ_WRITE, 0, len);
        ibuf = buf.asLongBuffer();
        ibuf.put(data, pos, rc);
        buf.force();
        fc.close();
        file.close();
        pos = rc;
        id++;
      }
    }

    public String toString() {
      StringBuffer buf = new StringBuffer();
      buf.append('[');
      for (int i = 0; i < cur; i++) {
        buf.append(data[i]);
        if (i != cur -1)
          buf.append(',');
      }
      buf.append(']');
      return buf.toString();
    }

  }

  public static final class DoubleVector extends Vector {
    double[] data;

    DoubleVector() {}

    public DoubleVector(int capacity) {
      data = new double[capacity];
      type = Column_Type.DOUBLE;
    }

    public DoubleVector(String fileName, int len) {
      this.fileName = fileName;
      loaded = false;
      cur = len;
      type = Column_Type.DOUBLE;
    }

    public DoubleVector(DoubleBuffer buf, long len) {
      data = new double[(int)(len/8)];
      buf.get(data);
      cur = data.length;
      type = Column_Type.DOUBLE;
    }

    public long size() {
      return 8*cur;
    }

    public void copy(Vector src) {
      super.copy(src);
      DoubleVector dsrc = (DoubleVector)src;
      System.arraycopy(dsrc.data, 0, data, 0, dsrc.data.length);
    }

    private void realloc() {
      double[] old_data = data;
      data = new double[get_new_size(data.length)];
      System.arraycopy(old_data, 0, data, 0, old_data.length);
    }

    public void append(Object data) {
      append(todouble(data));
    }

    public void append(double val) {
      if (cur >= data.length)
        realloc();
      this.data[cur++] = val;
      clear_index();
    }

    public IntVector filter(Term.Operator op, Object value) {
      IntVector index = new IntVector(256);
      double arg = todouble(value);

      for (int i = 0; i < cur; i++) {
        boolean ret;
        switch(op) {
          case EQUAL:
            ret = Table.equal(data[i], arg);
            break;
          case GREATER:
            ret = Table.greater(data[i], arg);
            break;
          case GREATEREQUAL:
            ret = Table.greaterequal(data[i], arg);
            break;
          case LESS:
            ret = Table.less(data[i], arg);
            break;
          case LESSEQUAL:
            ret = Table.lessequal(data[i], arg);            
            break;
          case NOTEQUAL:
            ret = Table.notequal(data[i], arg);            
          default:
            throw new TableException("nyi");
        }
        if (ret)
          index.append(i);
      }
      return index;
    }

    public boolean compare(Term.Operator op, int pos, Object arg) {
      boolean ret;
      switch(op) {
        case EQUAL:
          ret = equal(pos, arg);
          break;
        case GREATER:
          ret = greater(pos, arg);
          break;
        case GREATEREQUAL:
          ret = greaterequal(pos, arg);
          break;
        case LESS:
          ret = less(pos, arg);
          break;
        case LESSEQUAL:
          ret = lessequal(pos, arg);            
          break;
        case NOTEQUAL:
          ret = notequal(pos, arg);            
        default:
          throw new TableException("nyi");
      }
      return ret;
    }

    public boolean equal(int pos, Object value) {
      return data[pos] == todouble(value);
    }

    public boolean notequal(int pos, Object value) {
      return data[pos] != todouble(value);
    }

    public boolean greater(int pos, Object value) {
      return data[pos] > todouble(value);
    }

    public boolean less(int pos, Object value) {
      return data[pos] < todouble(value);
    }

    public boolean greaterequal(int pos, Object value) {
      return data[pos] >= todouble(value);
    }

    public boolean lessequal(int pos, Object value) {
      return data[pos] <= todouble(value);
    }

    public Object get(int position) {
      if (position >= cur || position < 0)
        return 0.0;
      return data[position];
    }

    public Object sum(int start, int end) {
      return Pexec.sum(data, start, end);
    }

    public Object avg(int start, int end) {
      return (double)Pexec.sum(data, start, end) / (end - start + 1);
    }

    public void sample(int[] index, Object[] data) {
      int len = 0;
      if (index == null || data == null || (len = index.length) != data.length)
        throw new TableException("length mismatch");

      for (int i = 0; i < len; i++) {
        data[i] = get(index[i]); 
      }
    }

    public Vector get(IntVector index) {
      int len = index.length();
      DoubleVector dv = new DoubleVector(index.length()); 
      if (len <= 0)
        return dv;

      for (int i = 0; i < len; i++) {
        dv.append(data[index.fast_get(i)]); 
      }
      return dv;
    }

    public Object first() {
      return get(0);
    }

    public Object last() {
      return get(cur -1);
    }

    public void rand(int size, int upper) {
      data = new double[size];
      cur = size;

      Rand rand = new Rand();

      for (int i = 0; i < size; i++)
        data[i] = 1.0 * (rand.nextInt() % upper);

    }

    public void range(int size, int step) {
      if (step < 1) 
        step = 1;
      data = new double[size];
      cur = size;

      for (int i = 0; i < size; i++)
        data[i] = i*step;

    }

    public Object sum(IntVector index) {
      double sum = 0;
      for (int i = 0; i < index.cur; i++)
        sum += data[index.data[i]];
      return sum;
    }

    public Object avg(IntVector index) {
      double sum = 0;
      for (int i = 0; i < index.cur; i++)
        sum += data[index.data[i]];
      return sum/index.cur;
    }

    public Object mov_avg(IntVector index, int window) {

      if (index != null) {
        DoubleVector dv = new DoubleVector(index.length());

        for (int i = 0; i < index.length(); i++) {
          int start = i - window + 1;
          double sum = 0;
          int count = 0;
          for (int j = start; j <= i; j++) {
            if (j >= 0) {
              sum += data[index.fast_get(j)];
              count++;
            }
          }
          dv.append(sum/count);
        }
        return dv;
      }
      else {
        DoubleVector dv = new DoubleVector(length());

        for (int i = 0; i < length(); i++) {
          int start = i - window + 1;
          double sum = 0;
          int count = 0;
          for (int j = start; j <= i; j++) {
            if (j >= 0) {
              sum += data[j];
              count++;
            }
          }
          dv.append(sum/count);
        }
        return dv;
      }
    }

    public Object sample(int window) {
      DoubleVector dv = new DoubleVector(window);
      Rand rand = new Rand();
      int pos = 0;
      int len = length();
      for (int i = 0; i < window; i++) {
        dv.append(data[pos]);
        pos += rand.nextInt()&0x0F;
        if (pos >= len)
          break;
      }
      return dv;
    }

    public Object sample(IntVector index, int window) {
      DoubleVector dv = new DoubleVector(window);

      Rand rand = new Rand();
      int pos = 0;
      int len = index.length();
      for (int i = 0; i < window; i++) {
        dv.append(data[index.fast_get(pos)]);
        if (rand.nextInt()%2 == 0)
          pos += 1;
        else
          pos += 2;
        if (pos >= len)
          break;
      }
      return dv;
    }

    public Object mov_sum(IntVector index, int window) {
      if (index != null) {
        DoubleVector dv = new DoubleVector(index.length());

        for (int i = 0; i < index.length(); i++) {
          int start = i - window + 1;
          double sum = 0;
          for (int j = start; j <= i; j++) {
            if (j >= 0)
              sum += data[index.fast_get(j)];
          }
          dv.append(sum);
        }
        return dv;
      }
      else {
        DoubleVector dv = new DoubleVector(length());

        for (int i = 0; i < length(); i++) {
          int start = i - window + 1;
          double sum = 0;
          for (int j = start; j <= i; j++) {
            if (j >= 0)
              sum += data[j];
          }
          dv.append(sum);
        }
        return dv;
      }
    }

    public Object min(IntVector index) {
      double min = Double.MAX_VALUE;

      double max = Double.MIN_VALUE;
      if (index != null) {
        int len = index.length();
        if (len <= 0)
          return 0;

        for (int i = 0; i < len; i++) {
          double v = data[index.fast_get(i)];
          min = min > v? v : min;
        }
        return min;
      } else {
        for (int i = 0; i < length(); i++) {
          double v = data[i];
          min = min > v? v : min;
        }
      }
      return min;
    }

    public Object min() {
      return min(null);
    }

    public Object max() {
      return max(null);
    }

    public Object max(IntVector index) {
      double max = Double.MIN_VALUE;
      if (index != null) {
        int len = index.length();
        if (len <= 0)
          return 0;
        for (int i = 0; i < len; i++) {
          double v = data[index.fast_get(i)];
          max = max < v? v : max;
        }
      } else {
        for (int i = 0; i < length(); i++) {
          double v = data[i];
          max = max < v? v : max;
        }
      }
      return max;
    }

    public Object plus(IntVector index, Object num) {
      int len = index.length();
      DoubleVector dv = new DoubleVector(256); 
      if (len <= 0)
        return dv;

      double val = todouble(num);

      for (int i = 0; i < len; i++) {
        double v = data[index.fast_get(i)];
        dv.append(v+val);
      }
      return dv;
    }

    private double todouble(Object num) {
      double val = 0.0;
      if (num instanceof Double)
        val = (Double)num;
      else if (num instanceof Integer)
        val = (Integer)num;
      else if (num instanceof Long)
        val = (Long)num;
      else 
        throw new TableException("data type");
      return val;
    }

    public Object plus(Object num) {
      double val = todouble(num);

      DoubleVector dv = new DoubleVector(256); 

      for (int i = 0; i < length(); i++) {
        dv.append(data[i]+val);
      }
      return dv;
    }

    public Object minus(Object num) {
      double val = todouble(num);

      DoubleVector dv = new DoubleVector(256); 

      for (int i = 0; i < length(); i++) {
        dv.append(data[i]-val);
      }
      return dv;
    }    

    public Object minus(IntVector index, Object num) {
      int len = index.length();
      DoubleVector dv = new DoubleVector(256); 
      if (len <= 0)
        return dv;

      double val = todouble(num);

      for (int i = 0; i < len; i++) {
        double v = data[index.fast_get(i)];
        dv.append(v-val);
      }
      return dv;
    }

    public String toString() {
      StringBuffer buf = new StringBuffer();
      buf.append('[');
      for (int i = 0; i < cur; i++) {
        buf.append(data[i]);
        if (i != cur -1)
          buf.append(',');
      }
      buf.append(']');
      return buf.toString();
    }

    static DoubleVector load(String fileName, MappedByteBuffer buf, int size) throws IOException {
      int count = size / 8;
      int totalSize = buf.getInt();
      double[] data = new double[totalSize];
      buf.asDoubleBuffer().get(data, 0, count);
      int id = 0;
      while (count < totalSize) {
        RandomAccessFile file = new RandomAccessFile(fileName+"__"+id, "r");
        FileChannel fc = file.getChannel();
        int len = (int)fc.size();
        buf = fc.map(MapMode.READ_ONLY, 0, len);
        len = len/8;
        double[] tempData = new double[len];
        buf.asDoubleBuffer().get(tempData, 0, len);
        System.arraycopy(tempData, 0, data, count, len);
        fc.close();
        file.close();
        count += len;
        id++;
      }
      DoubleVector v = new DoubleVector();
      v.cur = totalSize;
      v.type = Column_Type.DOUBLE;
      v.data = data;
      return v;
    }

    public void save(String fileName) throws IOException {
      RandomAccessFile file = new RandomAccessFile(fileName, "rw");
      FileChannel fc = file.getChannel();
      MappedByteBuffer buf = fc.map(MapMode.READ_WRITE, 0, 8);
      buf.putInt(type.ordinal());
      buf.putInt(cur);

      int count = cur;
      int threshold = 200000000;
      long start = 8;
      long len = 0;
      int pos = 0, rc = 0;

      if (count > threshold) {
        len = threshold*8;
        count -= threshold;
        rc = threshold;
      } else {
        rc = count;
        len = count*8;
        count = 0;
      }
      buf = fc.map(MapMode.READ_WRITE, start, len);
      DoubleBuffer dbuf = buf.asDoubleBuffer();
      dbuf.put(data, pos, rc);
      buf.force();
      fc.close();
      file.close();

      int id = 0;
      while (count > 0) {
        if (count > threshold) {
          len = threshold*8;
          count -= threshold;
          rc = threshold;
        }
        else {
          rc = count;
          len = count*8;
          count = 0;
        }
        file = new RandomAccessFile(fileName+"__"+id, "rw");
        fc = file.getChannel();
        buf = fc.map(MapMode.READ_WRITE, 0, len);
        dbuf = buf.asDoubleBuffer();
        dbuf.put(data, pos, rc);
        buf.force();
        fc.close();
        file.close();
        pos = rc;
        id++;
      }
    }

  }  

  public static final class VectorVector extends Vector {
    Vector[] data;

    public VectorVector(int capacity) {
      type = Column_Type.VECTOR;
      data = new Vector[capacity];
    }

    public long size() {
      return 8*data.length;
    }

    public void copy(Vector src) {
      super.copy(src);
      VectorVector isrc = (VectorVector)src; 
      System.arraycopy(isrc.data, 0, data, 0, isrc.data.length);
    }

    private void realloc() {
      Vector[] old_data = data;
      data = new Vector[get_new_size(data.length)];
      System.arraycopy(old_data, 0, data, 0, old_data.length);
    }

    public void append(Vector v) {
      if (cur >= data.length)
        realloc();
      this.data[cur++] = v;
      clear_index();
    }

    public void append(Object v) {
      append((Vector)v);
    }

    public Object get(int position) {
      if (position >= cur)
        return 0;
      return data[position];
    }

    public String toString() {
      StringBuffer buf = new StringBuffer();
      buf.append('[');
      for (int i = 0; i < cur; i++) {
        buf.append(data[i].toString());
        if (i != cur -1)
          buf.append(',');
      }
      buf.append(']');
      return buf.toString();
    }

  }


  final static class IntTuple {
    int[] tuple;

    public IntTuple(int len) {
      tuple = new int[len];
    }

    public int hashCode() {
      int hash = tuple[0];
      for (int i = 1; i < tuple.length; i++)
        hash ^= tuple[i];
      return hash;
    }

    public static boolean equals(IntTuple t1, IntTuple t2) {
        int len = t1.tuple.length;
        if (t2.tuple.length == len) {
          for (int i = 0; i < len; i++) {
            if (t1.tuple[i] != t2.tuple[i])
              return false;
          }
          return true;
        }
        return false;
    }

    public boolean equals(Object obj) {
      if (obj instanceof IntTuple) {
        IntTuple other = (IntTuple)obj;
        int len = other.tuple.length;
        if (tuple.length == len) {
          for (int i = 0; i < len; i++) {
            if (tuple[i] != other.tuple[i])
              return false;
          }
          return true;
        }
      }
      return false;
    }

    public String toString() {
      StringBuffer buf = new StringBuffer();

      for (int i = 0; i < tuple.length; i++) {
        buf.append(tuple[i]);
        buf.append('-');
      }

      return buf.toString();
    }
  }

  final static class Tuple {
    Object[] tuple;

    public Tuple(int len) {
      tuple = new Object[len];
    }

    public int hashCode() {
      int hash = tuple[0].hashCode();
      for (int i = 1; i < tuple.length; i++)
        hash ^= tuple[i].hashCode();
      return hash;
    }

    public boolean equals(Object obj) {
      if (obj instanceof Tuple) {
        Tuple other = (Tuple)obj;
        int len = other.tuple.length;
        if (tuple.length == len && hashCode() == other.hashCode()) {
          for (int i = 0; i < len; i++) {
            if (!tuple[i].equals(other.tuple[i]))
              return false;
          }
          return true;
        }
      }
      return false;
    }

    public String toString() {
      StringBuffer buf = new StringBuffer();

      for (int i = 0; i < tuple.length; i++) {
        buf.append(tuple[i].toString());
        buf.append('-');
      }

      return buf.toString();
    }
  }

  abstract class HashIndex {
    public abstract Dictionary<?, ?>  hashIndex();
    public String toString() {
      return hashIndex().toString();
    }
  }

  final class ObjectHashIndex extends HashIndex {
    Dictionary<Tuple, IntVector> hashIndex;

    public ObjectHashIndex(int default_size) {
      hashIndex = new Dictionary<Tuple, IntVector>(true, new Dictionary.TupleHashFunction(),
        new Dictionary.TupleHashEqualFunction(), default_size);
    }

    public void put(Tuple tuple, int index) {
      IntVector vec;
      int x = hashIndex.put(tuple);

      if ((vec = hashIndex.get_value(x)) == null) {
        vec = new IntVector(16); 
        hashIndex.put(x, vec);
      }
      vec.append(index);
    }

    public Dictionary<?, ?> hashIndex() {
      return hashIndex;
    }
  }

  final class IntHashIndex extends HashIndex {
    Dictionary<IntTuple, IntVector> hashIndex;

    public IntHashIndex(int default_size) {
      hashIndex = new Dictionary<IntTuple, IntVector>(true, new Dictionary.IntTupleHashFunction(),
        new Dictionary.IntTupleHashEqualFunction(), default_size);
    }

    public void put(IntTuple tuple, int index) {
      IntVector vec;
      int x = hashIndex.put(tuple);

      if ((vec = hashIndex.get_value(x)) == null) {
        vec = new IntVector(16); 
        hashIndex.put(x, vec);
      }
      vec.append(index);
    }

    public Dictionary<?, ?> hashIndex() {
      return hashIndex;
    }
  }



  static public class Term {
    Operator op;
    Object[] params;
    String col;
    public enum Operator {
      NONE, MONTH, DAY, DAY_OF_YEAR, YEAR, FIRST, LAST, AVG,
      SUM, MOV_SUM, MOV_AVG, MAX, MIN, NTH, PLUS, MINUS, PRODUCT,
      EQUAL, GREATER, LESS, GREATEREQUAL, LESSEQUAL, NOTEQUAL, INSTANT, SAMPLE, COUNT, RID, DISTINCT_COUNT
    };

    Term left, right;

    public Term(Operator op, Term term, String name) {
      this.col = name;
      this.op = op;
      left = term;
    }

    public Term(Operator op, Term term) {
      this.op = op;
      left = term;
    }

    public Term(Operator op, Term term1, Term term2) {
      this.op = op;
      left = term1;
      right = term2;
    }

    public Term(Operator op, Term term1, Term term2, String name) {
      this.col = name;
      this.op = op;
      left = term1;
      right = term2;
    }

    public Term(String col, Operator op) {
      this(col, op, null);
    }

    public Term(String col, Operator op, Object[] params) {
      this.col = col;
      this.op = op;
      if (params != null) {
        this.params = new Object[params.length];
        System.arraycopy(params, 0, this.params, 0, params.length);
      }
    }

  }

  final static public class ByTerm extends Term {
    public ByTerm(String col, Operator op) {
      super(col, op, null);
    }

    public ByTerm(String col, Operator op, Object[] params) {
      super(col, op, params);
    }
  }

  final static public class SelTerm extends Term {
    public SelTerm(String col, Operator op) {
      super(col, op, null);
    }

    public SelTerm(String col, Operator op, Object[] params) {
      super(col, op, params);
    }
  }

  final static class Pexec {
    private static int poolSize; 
    private static ExecutorService pool;
    private final static int threshold = 50000000;

    public static void start() {
      poolSize = Runtime.getRuntime().availableProcessors();
      pool = Executors.newFixedThreadPool(poolSize);
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {    
          shutdown();
        }
      });
    }

    public static void shutdown() {
      if (pool != null)
        pool.shutdownNow();
    }

    private static class DoubleArrayTask implements Callable<Double> {
      double[] data;
      int start;
      int end;

      DoubleArrayTask(double[] data, int start, int end) {
        this.data = data;
        this.start = start;
        this.end = end;
      }

      public Double call() {
        double sum = 0;
        for (int i = start; i <= end; i++)
          sum += data[i];
        return sum;
      }
    }

    private static void init() {
      if (pool == null)
        start();
    }

    public static double sum(double[] data, int start, int end) {

      double sum = 0;
      if (end - start < threshold) {
        for (int i = start; i <= end; i++)
          sum += data[i];
        return sum;
      }

      init();
      int nums = (end - start) / threshold; 
      int rem = (end - start) % threshold;

      ArrayList<DoubleArrayTask> tasks = new ArrayList<DoubleArrayTask>(nums);

      int from = start, to = start + rem;
      int n = nums;
      do {
        tasks.add(new DoubleArrayTask(data, from, to));
        from = to + 1;
        to = to + threshold;
      } while(n-->0); 


      try {
        for (Future<Double> f : pool.invokeAll(tasks))  {
          sum += f.get();
        }

      } catch (InterruptedException e) {
        sum = Double.NaN;
      } catch (ExecutionException e) {
        sum = Double.NaN;
      }
      return sum;
    }
  }


  public Table(String[] col_names, Vector[] vecs) {
    if (col_names == null || vecs == null || vecs.length != col_names.length)
      throw new TableException("table schema");
    int num_cols = col_names.length;
    int len = vecs[0].length();

    for (int i = 1; i < num_cols; i++)
      if (vecs[i].length() != len)
        throw new TableException("table schema length mismatch");

    this.col_names = new String[num_cols];
    System.arraycopy(col_names, 0, this.col_names, 0, num_cols);
    this.col_names_clone = new String[num_cols];
    System.arraycopy(col_names, 0, this.col_names_clone, 0, num_cols);

    col_map = new HashMap<String, Integer>();
    for (int i = 0; i < num_cols; i++) 
      col_map.put(col_names [i], i);

    this.col_types = new Column_Type[num_cols];
    for (int i = 0; i < num_cols; i++)
      this.col_types[i] = vecs[i].get_type();
    this.col_types_clone = new Column_Type[num_cols];
    System.arraycopy(this.col_types, 0, this.col_types_clone, 0, num_cols);

    cols = vecs;

    capacity = len;
    cur = len;

  }

  public Table(String[] col_names, Column_Type[] col_types) {
    this(col_names, col_types, 0);
  }

  public Table(String[] col_names, Column_Type[] col_types, int mem_budget) {
    if (col_names == null || col_types == null || col_types.length != col_names.length)
      throw new TableException("table schema");
    int num_cols = col_types.length;
    this.col_names = new String[num_cols];
    System.arraycopy(col_names, 0, this.col_names, 0, num_cols);
    this.col_names_clone = new String[num_cols];
    System.arraycopy(col_names, 0, this.col_names_clone, 0, num_cols);
    col_map = new HashMap<String, Integer>();
    for (int i = 0; i < num_cols; i++) 
      col_map.put(col_names [i], i);

    this.col_types = new Column_Type[col_types.length];
    System.arraycopy(col_types, 0, this.col_types, 0, num_cols);
    this.col_types_clone = new Column_Type[col_types.length];
    System.arraycopy(col_types, 0, this.col_types_clone, 0, num_cols);

    cols = new Vector[num_cols];

    capacity = estimate(mem_budget);
    alloc();
    cur = 0;
  }

  public String[] getSchemaNames() {
    System.arraycopy(this.col_names, 0, this.col_names_clone, 0, cols.length);
    return this.col_names_clone;
  }

  public Column_Type[] getSchemaTypes() {
    System.arraycopy(this.col_types, 0, this.col_types_clone, 0, cols.length);
    return this.col_types_clone;
  }

  void bindIndex(Table parent, HashIndex hashIndex) {
    this.parent = parent;
    this.hashIndex = hashIndex;
  }


  private int estimate(int mem_budget) {
    if (mem_budget == 0) 
      return 16;

    long mem_size = 1000000L * mem_budget; 
    long min = mem_size;
    for (int i = 0; i < cols.length; i++) {
      Column_Type col_type = col_types[i];
      long cur;
      switch (col_type) {
        case BOOLEAN:
          cur = mem_size;  
          break;
        case VARCHAR8:
          cur = mem_size / 8;
          break;
        case VARCHAR16:
          cur = mem_size / 16;
          break;
        case VARCHAR256:
          cur = mem_size / 256;
          break;
        case INT:
        case SYMBOL:
          cur = mem_size / 4;
          break;
        case DOUBLE:
          cur = mem_size / 8;
          break;
        case TIMESTAMP:
          cur = mem_size / 8;
          break;
        case VECTOR:
          cur = mem_size / 8; 
          break;
        default:
          throw new TableException("column type");
      }
      min = min > cur? cur : min;
    }
    return (int)min;
  }

  private void alloc() {
    for (int i = 0; i < cols.length; i++) {
      Column_Type col_type = col_types[i];
      switch (col_type) {
        case BOOLEAN:
          cols[i] = new BitVector(capacity);
          break;
        case VARCHAR8:
          cols[i] = new CharVector(8, capacity);
          break;
        case VARCHAR16:
          cols[i] = new CharVector(16, capacity);
          break;
        case VARCHAR256:
          cols[i] = new CharVector(256, capacity);
          break;
        case INT:
          cols[i] = new IntVector(capacity);
          break;
        case LONG:
          cols[i] = new LongVector(capacity);
          break;          
        case TIMESTAMP:
          cols[i] = new TimestampVector(capacity);
          break;
        case DOUBLE:
          cols[i] = new DoubleVector(capacity);
          break;
        case VECTOR:
          cols[i] = new VectorVector(capacity);
          break;
        case SYMBOL:
          cols[i] = new SymbolVector(capacity);
          break;
        default:
          throw new TableException("column type");
      }
    }
  }

  private void realloc() {
    int orig_capacity = capacity;
    capacity = Vector.get_new_size(capacity); 

    Vector[] old_cols = cols; 
    cols = new Vector[cols.length];
    alloc();
    for (int i = 0; i < cols.length; i++) 
      cols[i].copy(old_cols[i]);

  }

  private int getColumnId(String col_name) {
    Integer cid = col_map.get(col_name);
    if (cid == null)
      throw new TableException("column does not exist");
    return cid.intValue();
  }

  private int prepareData(int col) {
    if (cols[col].isLoaded())
      return col;
    cols[col] = cols[col].load();
    return col;
  }

  private int prepareData(String col_name) {
    return prepareData(getColumnId(col_name));
  }

  private Vector select(String col_name) {
    int cid = prepareData(col_name);
    return cols[cid];
  }

  private Vector select(int col_id) {
    prepareData(col_id);
    return cols[col_id];
  }

  public int column_count() {
    return cols.length;
  }

  public Vector columnAt(String col_name) {
    return select(col_name);
  }

  public Vector columnAt(int col_id) {
    return cols[col_id];
  }

  public Object sum(String col_name) {
    return select(col_name).sum(0, cur - 1);
  }

  public Object avg(String col_name) {
    return select(col_name).avg(0, cur - 1);
  }

  public Object max(String col_name) {
    return select(col_name).max();
  }

  public Object min(String col_name) {
    return select(col_name).min();
  }

  public Vector mov_avg(String col_name, int window) {
    return (Vector)(select(col_name).mov_avg((IntVector)null, window));
  }

  public Vector mov_sum(String col_name, int window) {
    return (Vector)(select(col_name).mov_sum((IntVector)null, window));
  }

  private boolean is_atom(Term term) {
    return term.left == null && term.right == null;
  }

  private Object eval(Term term, IntVector index) {
    Object val = null;
    Object val1 = null, val2 = null;

    if (term.left != null)
      val1 = eval(term.left, index);
    if (term.right != null)
      val2 = eval(term.right, index);

    int cid = -1;
    if (is_atom(term))
      cid = parent.getColumnId(term.col);
    switch (term.op) {
      case NONE:
        val = parent.cols[cid].get(index);
        break;
      case MAX:
        if (cid == -1)
          val = ((Vector)val1).max();
        else
          val = parent.cols[cid].max(index);
        break;
      case MIN:
        if (cid == -1)
          val = ((Vector)val1).min();
        else
          val = parent.cols[cid].min(index);
        break;
      case MINUS:
        if (val1 instanceof Vector)
          val = ((Vector)val1).minus(val2);
        else
          throw new TableException("col operator"); 
        break;
      default:
        throw new TableException("nyi");
    }
    return val;
  }

  private Table build_from_hashindex(Table tbl, Term[] selClause, int[] cids) {
    int width = cids.length;
    Object[] tuple = new Object[width];
    int count = limit;
    for (int i = 0; i < hashIndex.hashIndex().end(); i++) {
      if (!hashIndex.hashIndex().exists(i))
        continue;
      IntVector v = ((Dictionary<Tuple, IntVector>)(hashIndex.hashIndex())).get_value(i);
      for (int j = 0; j <width; j++) {
        Object val = null;
        if (cids[j] == -1) {
          val = eval(selClause[j], v);
        } else {
          switch (selClause[j].op) {
            case NONE:
              if (tbl.col_types[j] == Column_Type.VECTOR) {
                val = parent.select(cids[j]).get(v);
              } else 
                val = parent.select(cids[j]).get(v.data[0]); 
              break;
            case RID:
              val = v;
              break;
            case FIRST:
              val = parent.select(cids[j]).get(v.fast_first());
              break;
            case LAST:
              val = parent.select(cids[j]).get(v.fast_last());
              break;
            case MAX:
              val = parent.select(cids[j]).max(v);
              break;
            case MIN:
              val = parent.select(cids[j]).min(v);
              break;
            case SUM:
              val = parent.select(cids[j]).sum(v);
              break;
            case COUNT:
              val = v.length();
              break;
            case DISTINCT_COUNT:
              val = parent.select(cids[j]).distinct_count(v);
              break;
            case AVG:
              val = parent.select(cids[j]).avg(v);
              break;
            case MOV_SUM:
              val = parent.select(cids[j]).mov_sum(v, (Integer)selClause[j].params[0]);
              break;
            case MOV_AVG:
              val = parent.select(cids[j]).mov_avg(v, (Integer)selClause[j].params[0]);
              break;
            case SAMPLE:
              val = parent.select(cids[j]).sample(v, (Integer)selClause[j].params[0]);
              break;
            case INSTANT:
              if (tbl.col_types[j] == Column_Type.VECTOR) {
                val = parent.select(cids[j]).get_as_instant(v);
              } else 
                val = parent.select(cids[j]).get_as_instant(v.data[0]); 
              break;
            case DAY:
              if (tbl.col_types[j] == Column_Type.VECTOR) {
                val = parent.select(cids[j]).get_as_day(v);
              } else 
                val = parent.select(cids[j]).get_as_day(v.data[0]); 
              break;
            case DAY_OF_YEAR:
              if (tbl.col_types[j] == Column_Type.VECTOR) {
                val = parent.select(cids[j]).get_as_day_of_year(v);
              } else
                val = parent.select(cids[j]).get_as_day_of_year(v.data[0]); 
              break;
            case MONTH:
              if (tbl.col_types[j] == Column_Type.VECTOR) {
                val = parent.select(cids[j]).get_as_month(v);
              } else
                val = parent.select(cids[j]).get_as_month(v.data[0]); 
              break;
            case YEAR:
              if (tbl.col_types[j] == Column_Type.VECTOR) {
                val = parent.select(cids[j]).get_as_year(v);
              } else
                val = parent.select(cids[j]).get_as_year(v.data[0]); 
              break;
            default:
              throw new TableException("nyi");
          }
        }
        tuple[j] = val;
      }
      tbl.append(tuple);
      if (limit != -1 && --count <= 0)
        break;
    }

    return tbl;
  }

  private Table build_slice(Table tbl, Term[] selClause, int[] cids) {
    int width = cids.length;
    Object[] tuple = new Object[width];
    int len = limit == -1? cur : (limit > cur? cur : limit);
    boolean aggregated = false;
    for (int i = 0; i < len; i++) {
      for (int j = 0; j <width; j++) {
        Object val = null;
        switch (selClause[j].op) {
          case NONE:
          case FIRST:
          case LAST:
            val = select(cids[j]).get(i); 
            break;
          case RID:
            val = i;
            break;
          case YEAR:
            val = select(cids[j]).get_as_year(i);
            break;
          case MONTH:
            val = select(cids[j]).get_as_month(i);
            break;
          case DAY:
            val = select(cids[j]).get_as_day(i);
            break;
          case DAY_OF_YEAR:
            val = select(cids[j]).get_as_day_of_year(i);
            break;
          case INSTANT:
            val = select(cids[j]).get_as_instant(i);
            break;
          case MIN:
            val = select(cids[j]).min();
            aggregated = true;
            break;
          case MAX:
            val = select(cids[j]).max();
            aggregated = true;
            break;
          case DISTINCT_COUNT:
            val = select(cids[j]).distinct_count();
            aggregated = true;
            break;
          case COUNT:
            val = cur;
            aggregated = true;
            break;
          case SUM:
            val = select(cids[j]).sum(0, cur-1);
            aggregated = true;
            break;
          case SAMPLE:
          default:
            throw new TableException("nyi");
        }
        tuple[j] = val;
      }
      tbl.append(tuple);
      if (aggregated) 
        break; 
    }

    return tbl;
  }

  private String get_col_name(HashMap<String, Integer> name_hash, String base_name) {
    if (!name_hash.containsKey(base_name)) {
      name_hash.put(base_name, 0);
      return base_name;
    }
    else {
      int suffix = name_hash.get(base_name);
      name_hash.put(base_name, suffix + 1);
      return base_name + "-" + String.valueOf(suffix);
    }
  }

  private Table internal_select(Term[] selClause) 
      throws TableException {
    int width = selClause.length;
    String[] cols = new String[width];
    Column_Type[] col_types = new Column_Type[width];
    int[] cids = new int[width];

    HashMap<String, Integer> new_col_names = new HashMap<String, Integer>(); 
    for (int i = 0; i < width; i++) {
      Column_Type t;
      if (parent != null) {
        if (selClause[i].col == null || !parent.col_map.containsKey(selClause[i].col)) {
          cols[i] = get_col_name(new_col_names, selClause[i].col);
          cids[i] = -1; 
          t = Column_Type.DOUBLE; 
        } else {
          cids[i] = parent.col_map.get(selClause[i].col); 
          cols[i] = get_col_name(new_col_names, parent.col_names[cids[i]]);
          t = parent.col_types[cids[i]];
        }
      } else {
        cids[i] = getColumnId(selClause[i].col);
        cols[i] = get_col_name(new_col_names, col_names[cids[i]]);
        t = this.col_types[cids[i]];
      }

      switch(selClause[i].op) {
        case DISTINCT_COUNT:
          col_types[i] = Column_Type.INT;
          break;
        case COUNT:
          col_types[i] = Column_Type.INT;
          break;
        case AVG:
          col_types[i] = Column_Type.DOUBLE;
          break;
        case MOV_SUM:
        case MOV_AVG:
        case SAMPLE:
          col_types[i] = Column_Type.VECTOR;
          break;
        case RID:
          if (parent != null) 
            col_types[i] = Column_Type.VECTOR;
          else 
            col_types[i] = Column_Type.INT;
          break;
        case YEAR:
        case MONTH:
        case DAY:
        case DAY_OF_YEAR:
          if (parent != null && !col_map.containsKey(selClause[i].col)) 
            col_types[i] = Column_Type.VECTOR;
          else 
            col_types[i] = Column_Type.INT;
          break;
        case INSTANT:
          if (parent != null && !col_map.containsKey(selClause[i].col)) 
            col_types[i] = Column_Type.VECTOR;
          else 
            col_types[i] = Column_Type.LONG;
          break;
        case NONE:
          if (parent != null && !col_map.containsKey(selClause[i].col)) 
            col_types[i] = Column_Type.VECTOR;
          else 
            col_types[i] = t;
          break;
        default:
          col_types[i] = t;
      }
    }

    Table tbl = new Table(cols, col_types);
    if (hashIndex != null)
      build_from_hashindex(tbl, selClause, cids);
    else
      build_slice(tbl, selClause, cids);
    return tbl;
  }

  public void sample(String col_name, int[] index, Object[] data) {
    select(col_name).sample(index, data);
  }

  public Table select(Term[] selClause, int limit) {
    this.limit = limit;
    Table t = select(selClause);
    this.limit = -1;
    return t;
  }

  public Table select(Term[] selClause) {
    if (selClause == null)
      return null;
    return internal_select(selClause);
  }

  private IntHashIndex build_unary_index(SymbolVector v) {
    IntHashIndex hashIndex = new IntHashIndex(0); 
    hashIndex.hashIndex.n_buckets = v.index.dict.end(); 
    int count = hashIndex.hashIndex.n_buckets;
    int[] flags = new int[Dictionary.flag_size(count)];
    hashIndex.hashIndex.keys = new IntTuple[count];
    hashIndex.hashIndex.values = new IntVector[count];
    hashIndex.hashIndex.flags = flags;
    Dictionary.clear_flags(flags);
    for (int pos = 0 ; pos < count; pos++) {
      if (v.index.dict.exists(pos)) {
        IntTuple tuple = new IntTuple(1);
        tuple.tuple[0] = pos;
        hashIndex.hashIndex.keys[pos] = tuple;
        hashIndex.hashIndex.values[pos] = new IntVector(16);
        Dictionary.clear_both(flags, pos);
      }
    }

    for (int rowid = 0 ; rowid < cur; rowid++) 
      hashIndex.hashIndex.values[v.fast_get(rowid)].append(rowid);
    
    
    return hashIndex;
  }

  private IntHashIndex build_unary_index(Vector v) {
    IntHashIndex hashIndex = new IntHashIndex(128);
    for (int rowid = 0 ; rowid < cur; rowid++) {
      IntTuple tuple = new IntTuple(1);
      tuple.tuple[0] = ((Integer)v.get(rowid)).intValue(); 
      hashIndex.put(tuple, rowid);
    }
    return hashIndex;
  }

  private IntHashIndex build_unary_index_month(TimestampVector v) {
    IntHashIndex hashIndex = new IntHashIndex(128);
    for (int rowid = 0 ; rowid < cur; rowid++) {
      IntTuple tuple = new IntTuple(1);
      tuple.tuple[0] = v.fast_get_as_month(rowid); 
      hashIndex.put(tuple, rowid);
    }
    return hashIndex;
  }

  private IntHashIndex build_unary_index_year(TimestampVector v) {
    IntHashIndex hashIndex = new IntHashIndex(128);
    for (int rowid = 0 ; rowid < cur; rowid++) {
      IntTuple tuple = new IntTuple(1);
      tuple.tuple[0] = v.fast_get_as_year(rowid); 
      hashIndex.put(tuple, rowid);
    }
    return hashIndex;
  }

  private IntHashIndex build_unary_index_day(TimestampVector v) {
    IntHashIndex hashIndex = new IntHashIndex(128);
    for (int rowid = 0 ; rowid < cur; rowid++) {
      IntTuple tuple = new IntTuple(1);
      tuple.tuple[0] = v.fast_get_as_day(rowid); 
      hashIndex.put(tuple, rowid);
    }
    return hashIndex;
  }

  private IntHashIndex build_unary_index_instant(TimestampVector v) {
    IntHashIndex hashIndex = new IntHashIndex(128);
    for (int rowid = 0 ; rowid < cur; rowid++) {
      IntTuple tuple = new IntTuple(1);
      tuple.tuple[0] = (int)(v.fast_get_as_instant(rowid) & 0x00000000FFFFFFFF); 
      hashIndex.put(tuple, rowid);
    }
    return hashIndex;
  }

  private IntHashIndex build_unary_index_day_of_year(TimestampVector v) {
    IntHashIndex hashIndex = new IntHashIndex(128);
    for (int rowid = 0 ; rowid < cur; rowid++) {
      IntTuple tuple = new IntTuple(1);
      tuple.tuple[0] = v.fast_get_as_day_of_year(rowid); 
      hashIndex.put(tuple, rowid);
    }
    return hashIndex;
  }

  private HashIndex build_allInt_hashIndex(Vector[] vecs, Term[] byClause, Column_Type[] col_types) {
    int len = byClause.length;
    IntHashIndex hashIndex = null;
    if (len == 1) {
      Vector v = vecs[0];
      if (v.is_indexed())
        return v.get_index();
      Term.Operator op = byClause[0].op;
      switch(byClause[0].op) {
        case NONE:
          if (col_types[0] == Column_Type.SYMBOL)
            hashIndex = build_unary_index((SymbolVector)v);
          else 
            hashIndex = build_unary_index(v);
          break;
        case MONTH:
          hashIndex = build_unary_index_month((TimestampVector)v);
          break;
        case YEAR:
          hashIndex = build_unary_index_year((TimestampVector)v);
          break;
        case DAY:
          hashIndex = build_unary_index_day((TimestampVector)v);
          break;
        case INSTANT:
          hashIndex = build_unary_index_instant((TimestampVector)v);
          break;
        case DAY_OF_YEAR:
          hashIndex = build_unary_index_day_of_year((TimestampVector)v);
          break;
        default:
          throw new TableException("nyi");
      }
    } else {
      hashIndex = new IntHashIndex(128);
      for (int rowid = 0 ; rowid < cur; rowid++) {
        IntTuple tuple = new IntTuple(len);
        for (int j = 0; j < len; j++) {
          Vector v = vecs[j];
          switch(byClause[j].op) {
            case NONE:
              if (col_types[j] == Column_Type.SYMBOL)
                tuple.tuple[j] = ((SymbolVector)v).fast_get(rowid); 
              else 
                tuple.tuple[j] = ((Integer)v.get(rowid)).intValue();
              break;
            case MONTH:
              tuple.tuple[j] = ((TimestampVector)v).fast_get_as_month(rowid);
              break;
            case YEAR:
              tuple.tuple[j] = ((TimestampVector)v).fast_get_as_year(rowid);
              break;
            case DAY:
              tuple.tuple[j] = ((TimestampVector)v).fast_get_as_day(rowid);
              break;
            case INSTANT:
              tuple.tuple[j] = (int)(((TimestampVector)v).fast_get_as_instant(rowid) & 0x00000000FFFFFFFF);
              break;
            case DAY_OF_YEAR:
              tuple.tuple[j] = ((TimestampVector)v).fast_get_as_day_of_year(rowid);
              break;
            default:
              throw new TableException("nyi");
          }
        }
        hashIndex.put(tuple, rowid);
      }
    }
    if (len == 1) {
      Vector v = vecs[0];
      v.store_index(hashIndex);
    }
    return hashIndex;
  }

  private final static boolean equal(int v1, int v2) {
    return v1 == v2;
  }

  private final static boolean greater(int v1, int v2) {
    return v1 > v2;
  }

  private final static boolean less(int v1, int v2) {
    return v1 < v2;
  }

  private final static boolean greaterequal(int v1, int v2) {
    return v1 >= v2;
  }

  private final static boolean lessequal(int v1, int v2) {
    return v1 <= v2;
  }

  private final static boolean notequal(int v1, int v2) {
    return v1 != v2;
  }

  private final static boolean equal(long v1, long v2) {
    return v1 == v2;
  }

  private final static boolean notequal(long v1, long v2) {
    return v1 != v2;
  }

  private final static boolean greater(long v1, long v2) {
    return v1 > v2;
  }

  private final static boolean less(long v1, long v2) {
    return v1 < v2;
  }

  private final static boolean greaterequal(long v1, long v2) {
    return v1 >= v2;
  }

  private final static boolean lessequal(long v1, long v2) {
    return v1 <= v2;
  }

  private final static boolean equal(double v1, double v2) {
    return v1 == v2;
  }

  private final static boolean notequal(double v1, double v2) {
    return v1 != v2;
  }

  private final static boolean greater(double v1, double v2) {
    return v1 > v2;
  }

  private final static boolean less(double v1, double v2) {
    return v1 < v2;
  }

  private final static boolean greaterequal(double v1, double v2) {
    return v1 >= v2;
  }

  private final static boolean lessequal(double v1, double v2) {
    return v1 <= v2;
  }

  private IntVector filter(Term whereTerm) {
    if (whereTerm.params == null)
      throw new TableException("malformed");
    return select(whereTerm.col).filter(whereTerm.op, whereTerm.params[0]);
  }

  private IntVector filter(Term whereTerm1, Term whereTerm2) {
    if (whereTerm1.params == null || whereTerm2.params == null)
      throw new TableException("malformed");

    IntVector index = new IntVector(256);

    Vector v1 = select(whereTerm1.col);
    Vector v2 = select(whereTerm2.col);
    Object arg1 = whereTerm1.params[0];
    Object arg2 = whereTerm2.params[0];

    for (int row = 0; row < cur; row++) {
      if (v1.compare(whereTerm1.op, row, arg1) && v2.compare(whereTerm2.op, row, arg2))
        index.append(row);
    }
    return index;
  }

  private IntVector filter(Term[] whereClause) {
    IntVector index = new IntVector(256);

    for (int row = 0; row < cur; row++) {
      boolean f = true;
      int i = 0;
      for (i = 0; i < whereClause.length; i++) {
        if (select(whereClause[i].col).compare(whereClause[i].op, row, whereClause[i].params[0]))
          continue;
      }
      if (i >= whereClause.length)
        index.append(row);
    }
    return index;
  }

  public Table where(Term[] whereClause) {
    if (whereClause == null)
      return this;
    Table tbl = new Table(col_names, col_types);

    IntVector index = null;

    switch (whereClause.length) {
      case 1:
        index = filter(whereClause[0]);
        break;
      case 2:
        index = filter(whereClause[0], whereClause[1]);
        break;
      default:
        index = filter(whereClause);
        break;
    }

    for (int i = 0; i < tbl.cols.length; i++)
      tbl.cols[i] = select(i).get(index);
    tbl.cur = tbl.capacity = index.length();
    return tbl;
  }

  public Table groupby(Term[] byClause) {
    if (byClause == null)
      return this;

    HashIndex hashIndex;
    int len = byClause.length;
    Vector[] vecs = new Vector[len];
    Column_Type[] col_types = new Column_Type[len];
    String[] col_names = new String[len];
    boolean allInts = true;
    for (int i = 0; i < len; i++) {
      col_types[i] = this.col_types[getColumnId(byClause[i].col)];
      if (!(col_types[i] == Column_Type.SYMBOL || col_types[i] == Column_Type.TIMESTAMP || col_types[i] == Column_Type.INT)) {
        allInts = false;
      }
      col_names[i] = byClause[i].col;
      vecs[i] = select(col_names[i]);
    }
    long t1 = System.nanoTime();
    if (allInts) {
      hashIndex = build_allInt_hashIndex(vecs, byClause, col_types);
    } else {
      hashIndex = new ObjectHashIndex(128); 
      for (int rowid=0 ; rowid < cur; rowid++) {
        Tuple tuple = new Tuple(len);
        for (int j = 0; j < len; j++) {
          Vector v = vecs[j];
          switch(byClause[j].op) {
            case NONE:
              if (col_types[j] == Column_Type.SYMBOL)
                tuple.tuple[j] = ((SymbolVector)v).fast_get(rowid); 
              else 
                tuple.tuple[j] = v.get(rowid);
              break;
            case MONTH:
              if (col_types[j] == Column_Type.TIMESTAMP) 
                tuple.tuple[j] = ((TimestampVector)v).get_as_month(rowid);
              else
                throw new TableException("col type mismatch");
              break;
            case DAY:
              if (col_types[j] == Column_Type.TIMESTAMP)
                tuple.tuple[j] = ((TimestampVector)v).get_as_day(rowid);
              else
                throw new TableException("col type mismatch");
              break;
            case YEAR:
              if (col_types[j] == Column_Type.TIMESTAMP)
                tuple.tuple[j] = ((TimestampVector)v).get_as_year(rowid);
              else
                throw new TableException("col type mismatch");
              break;
            default:
              throw new TableException("nyi");
          }
        }
        ((ObjectHashIndex)hashIndex).put(tuple, rowid);
      }
    }
    long t2 = System.nanoTime();
    Table tbl = new Table(col_names, col_types);
    tbl.bindIndex(this, hashIndex);

    return tbl;
  }

  public Table groupby(String[] col_names) {
    if (col_names == null)
      return this;

    ObjectHashIndex hashIndex = new ObjectHashIndex(256);
    int len = col_names.length;
    for (int rowid=0 ; rowid < cur; rowid++) {
      Tuple tuple = new Tuple(len);
      for (int j = 0; j < len; j++) {
        Vector v = select(col_names[j]);
        tuple.tuple[j] = v.get(rowid);
      }
      hashIndex.put(tuple, rowid);
    }
    Column_Type[] col_types = new Column_Type[len];
    for (int i = 0; i < len; i++) {
      col_types[i] = this.col_types[getColumnId(col_names[i])];
    }

    Table tbl = new Table(col_names, col_types);
    tbl.bindIndex(this, hashIndex);
    return tbl;
  }

  public int width() {
    return cols == null? 0 : cols.length;
  }

  public int count() {
    return cur;
  }

  public void append(Object[] data) {
    if (cur >= capacity)
      realloc();
    for (int i = 0; i < cols.length; i++)  
      cols[i].append(data[i]);
    
    cur++;
  }

  public void put(int position, Object[] data) {

  }

  private void checkCols() {
    for (int i = 0; i < cols.length; i++) {
      if (!cols[i].isLoaded())
        cols[i] = cols[i].load();
    }
  }
  
  public Object[] get(int position) {
    checkCols();
    Object[] row = new Object[cols.length];
    for (int i = 0; i < cols.length; i++) 
      row[i] = cols[i].get(position);
    return row;
  }

  public static Table quick_load(String dirName) throws IOException {
    File db = new File(dirName);
    if (db.exists()) {
      LineNumberReader symReader = new LineNumberReader(new FileReader(new File(dirName + File.separator + ".sym")));
      String col;
      ArrayList<String> cols = new ArrayList<String>();
      ArrayList<Vector> vecs = new ArrayList<Vector>();
      ArrayList<Column_Type> col_types = new ArrayList<Column_Type>();
      while((col = symReader.readLine()) != null) {
        cols.add(col);
        Vector v = Vector.quick_load(dirName + File.separator + col);
        vecs.add(v);
        col_types.add(v.get_type());
      }
      symReader.close();
      String[] names = new String[cols.size()];
      Column_Type[] types = new Column_Type[col_types.size()];
      Table tbl = new Table(cols.toArray(names), col_types.toArray(types));
      tbl.cols = new Vector[vecs.size()];
      vecs.toArray(tbl.cols);
      tbl.capacity = tbl.cols[0].length();
      tbl.cur = tbl.capacity;
      return tbl;
    }
    return null;
  }


  public static Table load(String dirName) throws IOException {
    File db = new File(dirName);
    if (db.exists()) {
      LineNumberReader symReader = new LineNumberReader(new FileReader(new File(dirName + File.separator + ".sym")));
      String col;
      ArrayList<String> cols = new ArrayList<String>();
      ArrayList<Vector> vecs = new ArrayList<Vector>();
      ArrayList<Column_Type> col_types = new ArrayList<Column_Type>();
      while((col = symReader.readLine()) != null) {
        cols.add(col);
        Vector v = Vector.load(dirName + File.separator + col);
        vecs.add(v);
        col_types.add(v.get_type());
      }
      symReader.close();
      String[] names = new String[cols.size()];
      Column_Type[] types = new Column_Type[col_types.size()];
      Table tbl = new Table(cols.toArray(names), col_types.toArray(types));
      tbl.cols = new Vector[vecs.size()];
      vecs.toArray(tbl.cols);
      tbl.capacity = tbl.cols[0].length();
      tbl.cur = tbl.capacity;
      return tbl;
    }
    return null;
  }

  public void save(String dirName) throws IOException {
    File db = new File(dirName);

    db.mkdirs(); 
    PrintWriter symout = new PrintWriter(new File(dirName+ File.separator + ".sym"));
    try {
      for (int i = 0; i < col_names.length; i++) {
        symout.println(col_names[i]);
        cols[i].save(dirName + File.separator + col_names[i]);
      }
    } finally {
      symout.flush();
      symout.close();
    }

  }

  public static void shutdown() {
    Pexec.shutdown();
  }

  public Object first() {
    return null;
  }


  public ISeq next() {
    return null;
  }

  public ISeq more() {
    return null;
  }

  public Table cons(Object o) {
    return null;
  }

  public Table seq() {
    return null;
  }

  public IPersistentCollection empty() {
    return null; 
  }

  public boolean equiv(Object o) {
    return false;
  }

  public String toString() {
    StringBuffer buf = new StringBuffer();
    for(int i = 0; i < count(); i++) {
      Object[] row = get(i);
      for (Object o : row) {
        if (o instanceof char[])
          buf.append(new String((char[])o));
        else
          buf.append(o);
        buf.append("\t");
      }
      buf.append("\n");
    }
    buf.append("\n");
    return buf.toString();
  }

  public IPersistentMap meta(){
    return null;
  }

  public Table withMeta(IPersistentMap meta) {
    return null;
  }

}
