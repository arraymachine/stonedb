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

public class StoneEnum extends StoneList {

  static StoneList domains = new StoneList();
    
  private StoneList.Long tbl;

  public StoneEnum(StoneList.Symbol domain) {
    this(domain, domain);
  }

  public StoneEnum(StoneList domain) {
    this((StoneList)domain, (StoneList)domain);
  }

  public StoneEnum(StoneObject so) {
    this(so.address);
  }
  
  public StoneEnum(long address) {
    this.address = address;
    readFields();
  }

  protected long byteOffset(long n) {
    return n << 3;
  }
  
  public void writeFields() {
    dict.writeFields();
    pokeLong(0, dict.address);
    pokeLong(1, tbl.address);
  }

  public void readFields() {
    tbl = new StoneList.Long(peekLong(1), (byte)-1);
  }

  StoneEnum(byte t, StoneList.Long tbl) {
    super(t, 2);
    this.tbl = tbl;
  }

  public StoneEnum(StoneList.Symbol domain, StoneList.Symbol data) {
    this((StoneList)domain, (StoneList)data);
  }

  public StoneEnum(StoneList domain, StoneList data) {
    super(sdb.engine.StoneLisp.Type.Enum.asByte(), 2);
    byte enum_id;
    if ((enum_id = (byte)domains.shallowFind(domain)) == domains.length())
      domains.append(domain);
    build(domain, data);
    putType((byte)(sdb.engine.StoneLisp.Type.Enum.asByte() + enum_id));
    assert getType() <= 67: "enum type out of range";
  }

  private void build(StoneList domain, StoneList data) {
    if (tbl != null) 
      tbl.release();

    StoneList dsyms = StoneFuncs.distinct(domain);
    long len = dsyms.length();

    dsyms.release();

    tbl = new StoneList.Long(data.length());

    for (long pos = 0; pos < data.length(); pos++) {
      StoneObject d = data.getObject(pos);
      if (!(d instanceof StoneObject.Symbol)) {
        tbl.release();
        throw new SException("type");
      }
    }
  }

  public boolean equals(Object obj) {
    if (obj instanceof StoneEnum)
      return equals(this, (StoneEnum)obj);
    return false;
  }

  public static boolean equals(StoneEnum enum1, StoneEnum enum2) {
    return enum1.getType() == enum2.getType();
  }

  public void release() {
    if (decRefCount() > 0) 
      return;
    if (dict != null) {
      dict.release();
      dict = null;
    }
    if (tbl != null) {
      tbl.release();
      tbl = null;
    }
  }

  public java.lang.String toString() {
    long len = length();
    len = len > PrintSize? PrintSize : len;

    StringBuffer buf = new StringBuffer();
    buf.append("(enum:");
    StoneList.Long.ListIter iter = tbl.new ListIter();
    StoneList domain = dict.get_container();
    while (iter.hasNext() && len-->0) {
      buf.append(domain.getObject(iter.getLong()));
      buf.append(' ');
      iter.move();
    }
    if (length() > PrintSize)
      buf.append(" ...");
    buf.append(")");
    return buf.toString();
  }

  public long length() {
    return tbl.length();
  }

  public StoneObject getObject(long n) {
    return dict.get_container().getObject(tbl.get(n));
  }

  public StoneObject.Symbol get(long n) {
    if (n >= length()) 
      throw new SException("length");
    return (StoneObject.Symbol)dict.get_container().getObject(tbl.get(n));
  }

  public void putObject(long n, StoneObject obj) {
    if (n >= length()) 
      throw new SException("length");
    if (obj.getType() != Type.Symbol.asScalar()) 
      throw new SException("type");
    long k = tbl.get(n);
    dict.get_container().putObject(k, obj);
  }

  public void append(StoneObject obj) {
    if (obj.getType() != Type.Symbol.asScalar()) 
      throw new SException("type");

    long x = dict.get(obj); 
    if (dict.isExisting(x)) 
      tbl.append(dict.get_value(x));
    else {
      long len = dict.get_container().length();
      dict.get_container().append(obj);
      dict.put(len);
      tbl.append(len);
    }
  }

  public StoneList getList(StoneList.Int index) {
    long len = index.length(); 
    if (len == 0) 
      return StoneList.Empty;
    StoneList.Long list = new StoneList.Long(len);
    StoneList c = dict.get_container();
    for (long i = 0; i < len; i++) 
      list.put(i, tbl.get(index.get(i)));
    
    return new StoneEnum(getType(), dict, list);

  }

  public StoneList getList(StoneList.Long index) {
    long len = index.length(); 
    if (len == 0) 
      return StoneList.Empty;
    StoneList.Long list = new StoneList.Long(len);
    StoneList c = dict.get_container();
    for (long i = 0; i < len; i++) 
      list.put(i, tbl.get(index.get(i)));
    
    return new StoneEnum(getType(), dict, list);
  }
}
