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

import java.util.concurrent.ConcurrentHashMap;

public final class StoneMetaTable {

  private static Logger logger = Logger.getLogger(StoneMetaTable.class.getName());
  public final static class MetaInfo {
    String tableName;
    boolean isDistributed;
    Table table;

    public MetaInfo(String name, boolean isDistributed, Table table) {
      this.tableName = name;
      this.isDistributed = isDistributed;
      this.table = table;
    }

    public boolean isDistributed() { return this.isDistributed; }
    public Table getTable() { return this.table; }
    public void clear() { this.table = null; }
  }

  private ConcurrentHashMap<String, MetaInfo> _metaTable;
 
  public StoneMetaTable() {
   _metaTable  = new ConcurrentHashMap<String, MetaInfo>(10);
  }

  public synchronized boolean add(String name, boolean distributed) {
    return add(name, distributed, null);
  }

  public synchronized boolean add(String name, boolean distributed, Table table) {
    try {
      _metaTable.put(name, new MetaInfo(name, distributed, table));
      return true;
    }
    catch (NullPointerException e) {
      return false;
    }
  }

  public synchronized MetaInfo get(String name) {
    return _metaTable.get(name);
  }

  public synchronized boolean remove(String name) {
    MetaInfo info = _metaTable.remove(name);
    if (info != null) {
      info.clear();
      return true;
    }
    
    return false;
  }

  public static boolean isDistributed(MetaInfo info) {
    if (info == null)
      return false;
    return info.isDistributed();
  }

  public synchronized int size() {
    return _metaTable.size();
  }
}
