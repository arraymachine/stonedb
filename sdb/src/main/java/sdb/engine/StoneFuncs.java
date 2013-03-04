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

class StoneFuncs {

  public static StoneList distinct(StoneList data) {
    StoneList list = new StoneList();
    for (long i = 0; i < data.length(); i++) {
      if (dict.isNew(dict.put(i)))
        list.append(data.getObject(i));
    }
    dict.release();
    return list;
  }

  public static StoneObject evalObject(StoneLisp.Environment env, StoneObject obj) {
    StoneObject ret = StoneObject.Null;
    try {
      if (obj.isList())  {
        if (StoneObject.Type.getType(obj.getType()) == StoneObject.Type.Char) {
          ret = env.get(obj);
          ret.retain();
        } else 
          ret = obj.eval(env, null);
      }
      else {
        ret = obj.eval();
      }
    } catch (StoneLisp.LangException e) {
      env.debugError(e);
    }
    return ret;
  }
}
