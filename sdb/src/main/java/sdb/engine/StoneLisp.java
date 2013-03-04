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

import java.util.HashMap;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.LinkedList;
import java.lang.ref.WeakReference;

public class StoneLisp {

  public static final String version = "StoneLisp-0.1";

  private static ThreadLocal<StoneLisp> lisp = new ThreadLocal<StoneLisp>() {
    @Override 
    protected StoneLisp initialValue() {
      return new StoneLisp();
    }
  }; 

  public static enum Type { 
    Null((byte)0), Byte((byte)1), Char((byte)2), Short((byte)3), Int((byte)4), Float((byte)5), Double((byte)6), Long((byte)7), Symbol((byte)8), List((byte)9), 
      Timestamp((byte)20), DateTime((byte)21), Date((byte)22), Day((byte)23), Hour((byte)24), Minute((byte)25), Second((byte)26), 
      Enum((byte)30), 
      Dictionary((byte)99), Table((byte)100), Lambda((byte)101);    
    
    private final byte type;
    
    Type(byte type) {
      this.type = type; 
    }

    byte asByte() {
      return type;
    }

    byte asList() {
      return type;
    }

  }

  private boolean debug;
  private boolean verbose;
  private Environment env;
  private Reader reader; 
  PrintStream out = System.out;
  private LinkedList<WeakReference<StoneObject> > objs;
  private StoneSocket.Server srv;
  private StoneQueue inputq;

  public static StoneLisp get() {
    return lisp.get();
  }

  StoneLisp() {
    init();
  }

  private void init() {
    debug = false;
    verbose = true;
    env = new Environment();
    setupBuiltin();
    objs = new LinkedList<WeakReference<StoneObject>>();
  }
  
  public void register(StoneObject so) {
    WeakReference<StoneObject> ref = new WeakReference<StoneObject>(so);
    objs.add(ref);
  }

  public void drain() {
    if (objs != null) {
      for (WeakReference<StoneObject> ref : objs) {
        StoneObject so = ref.get();
        so.release();
        ref.clear();
      }
      objs = null;
    }
  }

  public void reset() {
    env.release();
    drain();
    init();
  }
  

  private static final StoneObject[] funcs = new StoneObject[]{
    new StoneList.String("eq"),
    new StoneList.String("quote"),
    new StoneList.String("first"),
    new StoneList.String("rest"),
    new StoneList.String("atoms"),
    new StoneList.String("cond"),
    new StoneList.String("join"),
    new StoneList.String("t"),
    new StoneList.String("nil"),
    new StoneList.String("plus"),
    new StoneList.String("+"),
    new StoneList.String("minus"),
    new StoneList.String("-"),
    new StoneList.String("setq"),
    new StoneList.String("def"),
    new StoneList.String("print"),
    new StoneList.String("println"),
    new StoneList.String("distinct"),
    new StoneList.String("type"),
    new StoneList.String("cast"),
    new StoneList.String("map"),
    new StoneList.String("reduce"),
    new StoneList.String("cross"),
    new StoneList.String("scan"),
    new StoneList.String("scan-back"),
    new StoneList.String("table"),
    new StoneList.String("insert"),
    new StoneList.String("upsert"),
    new StoneList.String("plus_upsert"),
    new StoneList.String("rand"),
    new StoneList.String("count"),
    new StoneList.String("enum"),
    new StoneList.String("index"),
    new StoneList.String("dict"),
    new StoneList.String("handle"),
    new StoneList.String("send"),
    new StoneList.String("sendrecv"),
    new StoneList.String("__global__")
  };

  private void setupBuiltin() {
    env.set(new StoneList.String("eq"), new Eq());
    env.set(new StoneList.String("quote"), new Quote());
    env.set(new StoneList.String("first"), new First());
    env.set(new StoneList.String("rest"), new Rest());
    env.set(new StoneList.String("atoms"), new Atoms());
    env.set(new StoneList.String("cond"), new Cond());
    env.set(new StoneList.String("join"), new Join());
    env.set(new StoneList.String("t"), StoneObject.True);
    env.set(new StoneList.String("nil"), StoneObject.False);
    env.set(new StoneList.String("plus"), new Plus());
    env.set(new StoneList.String("+"), new Plus());
    env.set(new StoneList.String("minus"), new Minus());
    env.set(new StoneList.String("-"), new Minus());
    env.set(new StoneList.String("setq"), new Setq());
    env.set(new StoneList.String("def"), new Def());
    env.set(new StoneList.String("print"), new Print());
    env.set(new StoneList.String("println"), new Println());
    env.set(new StoneList.String("distinct"), new Distinct());
    env.set(new StoneList.String("table"), new Table());
    env.set(new StoneList.String("insert"), new Insert());
    env.set(new StoneList.String("upsert"), new Upsert());
    env.set(new StoneList.String("plus_upsert"), new PlusUpsert());
    env.set(new StoneList.String("rand"), new Rand());
    env.set(new StoneList.String("count"), new Count());
    env.set(new StoneList.String("enum"), new SymEnum());
    env.set(new StoneList.String("index"), new Index());
    env.set(new StoneList.String("dict"), new Dict());
    env.set(new StoneList.String("handle"), new Handle());
    env.set(new StoneList.String("send"), new Send());
    env.set(new StoneList.String("sendrecv"), new SendRecv());
    env.set(new StoneList.String("__global__"), env); 

  }

  public static class LangException extends RuntimeException {
    public LangException(String msg) {
      super(msg);
    }

    public LangException(Exception e) {
      super(e);
    }
  }

  public void process(InputStream in, PrintStream out, StoneSocket.Server srv) throws IOException {
    this.srv = srv;
    if (srv == null) {
      BufferedReader rd = new BufferedReader(new InputStreamReader(in));
      String source; 
      this.out = out;
      reader = new Reader();
      char[] buffer = new char[16*1024];
      int count = -1;
      while ((count = rd.read(buffer, 0, buffer.length)) != -1) {
        String code = new String(buffer, 0, count);
        StoneObject so = eval(code);
        out.println(so);
        so.release();
      }
      return;
    } 

    inputq = new StoneQueue(1024);
    this.srv.listen(inputq);
    Thread interp = new Thread(new Runnable() {
        private void print(Object h, Object ret) {
          if (h instanceof PrintStream)
            ((PrintStream)h).println(ret);
          else {
            System.out.println("output " + ret + " to back remote socket" + h);
          }
        }

        public void run() {
          while (true) {
            long avail;
            while((avail = inputq.availableToPoll()) == 0) {
              try {
                Thread.currentThread().sleep(0);
              } catch (InterruptedException ioe) {
              }
            }
            for(int i = 0; i < avail; i++) {
              StoneQueue.StoneObjectBuilder sb = inputq.poll();
              StoneObject expr = sb.get();
              try {
                StoneObject ret = expr.eval(env, null);
                print(sb.getHandle(), ret);
                ret.release();
              } catch (LangException exp) {
                print(sb.getHandle(), exp);
              }
            }
            inputq.donePolling();
          }
        }
      }, "stonelisp-executor");

    interp.start();

    BufferedReader rd = new BufferedReader(new InputStreamReader(in));
    String source; 
    this.out = out;
    out.println("srv: " + srv);
    char[] buffer = new char[16*1024];
    int count = -1;
    Reader reader = new Reader();
    while ((count = rd.read(buffer, 0, buffer.length)) != -1) {
      String code = new String(buffer, 0, count);
      StoneObject expr = reader.parse(code);
      StoneQueue.StoneObjectBuilder sb;
      while((sb = inputq.nextToDispatch()) == null) {
      }
      sb.set(expr);
      sb.setHandle(out);
      inputq.flush();
    }

  }

  public StoneObject eval(String source) {
    if (reader == null) 
      reader = new Reader();
    StoneObject ret = StoneObject.Null;
    StoneObject expr = reader.parse(source);
    while (!expr.equals(StoneObject.Null)) {
      ret.release();
      try {
        ret = expr.eval(env, null);
      } catch (LangException exp) {
        out.println(exp);
      }
      expr = reader.parse();
    }
    return ret;
  }


  public class Environment extends StoneObject {
    HashMap<StoneObject, StoneObject> binds;
    Environment parent;
    int level;
    boolean verbose = true;

    public Environment() {
      binds = new HashMap<StoneObject, StoneObject>();
      level = 0;
    }

    public void release() {
      env.set(new StoneList.String("__global__"), StoneObject.Null); 
      for (StoneObject so : binds.keySet()) {
        so.release();
      }
      for (StoneObject so : binds.values()) {
        so.release();
      }
      super.release();
      parent = null;
    }

    public Environment(Environment parent) {
      this.parent = parent;
      binds = new HashMap<StoneObject, StoneObject>();
      level = parent == null? 0 : parent.level++;
    }

    public boolean isDefined(StoneObject key) {
      if (binds.containsKey(key)) 
        return true;
      if (parent != null)
        return parent.isDefined(key);
      return false;
    }

    public StoneObject get(StoneObject key) {
      if (binds.containsKey(key))
        return binds.get(key);
      else if (parent != null)
        return parent.get(key);

      return StoneObject.Null;
    }

    public void set(StoneObject key, StoneObject value) {
      if (binds.containsKey(key)) {
        StoneObject oldval = binds.get(key);
        if (oldval != this)  
          oldval.release();
        binds.put(key, value);
        key.retain();
        value.retain();
      }
      else if (parent != null)
        parent.set(key, value);
      else {
        binds.put(key, value); 
        key.retain();
        value.retain();
      }
    }

    public Environment push() {
      return new Environment(this);
    }

    public Environment pop() {
      return parent;
    }

    public void debugError(LangException exp) {
      if (verbose)
        exp.printStackTrace();
      else 
        out.println(exp.getMessage());
    }
  }

  enum NumType {NaN, Hex, Int, Long, Float};

  private static NumType getNumType(String str) {
    char[] chars = str.toCharArray();
    int sz = chars.length;
    boolean hasExp = false;
    boolean hasDecPoint = false;
    boolean allowSigns = false;
    boolean foundDigit = false;
    int start = (chars[0] == '-') ? 1 : 0;
    if (sz > start + 1) {
      if (chars[start] == '0' && chars[start + 1] == 'x') {
        int i = start + 2;
        if (i == sz) {
          return NumType.NaN; 
        }
        for (; i < chars.length; i++) {
          if ((chars[i] < '0' || chars[i] > '9')
              && (chars[i] < 'a' || chars[i] > 'f')
              && (chars[i] < 'A' || chars[i] > 'F')) {
            return NumType.NaN;
          }
        }
        return NumType.Hex;
      }
    }
    sz--; 
    int i = start;
    while (i < sz || (i < sz + 1 && allowSigns && !foundDigit)) {
      if (chars[i] >= '0' && chars[i] <= '9') {
        foundDigit = true;
        allowSigns = false;

      } else if (chars[i] == '.') {
        if (hasDecPoint || hasExp) {
          return NumType.NaN;
        }
        hasDecPoint = true;
      } else if (chars[i] == 'e' || chars[i] == 'E') {
        if (hasExp) {
          return NumType.NaN;
        }
        if (!foundDigit) {
          return NumType.NaN;
        }
        hasExp = true;
        allowSigns = true;
      } else if (chars[i] == '+' || chars[i] == '-') {
        if (!allowSigns) {
          return NumType.NaN;
        }
        allowSigns = false;
        foundDigit = false; 
      } else {
        return NumType.NaN;
      }
      i++;
    }
    if (i < chars.length) {
      if (chars[i] >= '0' && chars[i] <= '9') {
        return (hasExp || hasDecPoint)? NumType.Float : NumType.Int;
      }
      if (chars[i] == 'e' || chars[i] == 'E') {
        return NumType.NaN;
      }
      if (!allowSigns
          && (chars[i] == 'd'
              || chars[i] == 'D'
              || chars[i] == 'f'
              || chars[i] == 'F')) {
        
        return foundDigit? NumType.Float : NumType.NaN;
      }
      if (chars[i] == 'l'
          || chars[i] == 'L') {
        if (foundDigit && !hasExp)
          return (hasExp || hasDecPoint)? NumType.Float : NumType.Long;
        return NumType.NaN;
      }
      return NumType.NaN;
    }
    if(!allowSigns && foundDigit)
      return NumType.Float;
    return NumType.NaN;
  }

  static class Reader {
    String source;
    int index;
    int length;

    public final static char[] specials = new char[]{'(',')', '[',']'}; 

    public Reader() {
    }

    public StoneObject parse(String input) {
      source = input;
      index = 0;
      length = input.length();
      return parse();
    }

    public StoneObject parse() {
      StoneObject token = tokenize();

      if (token.equals(CloseParen)) {
        token.release();
        throw new LangException("syntax error: ')'");
      }

      StoneList expr = new StoneList();
      if (token.equals(OpenParen)) {
        token = tokenize();

        while (!token.equals(CloseParen)) {
          if (token.equals(OpenParen)) {
            prev();
            expr.append(parse());
          } else if (token == StoneObject.Null) {
            expr.release();
            throw new LangException("invalid end of expression");
          } else
            expr.append(token);
          token = tokenize();
        }
        return expr;
      }
      return token;
    }

    public static boolean isSpecial(char ch) {
      for (char sc : specials) {
        if (ch == sc)
          return true;
      }
      return false;
    }

    public static boolean isDelimiter(char ch) {
      for (char sc : specials) {
        if (ch == sc)
          return true;
      }

      return Character.isWhitespace(ch);
    }

    private char current() {
      return source.charAt(index);
    }

    private void next() {
      index++;
    }

    private void prev() {
      index--;
    }

    private StoneObject convertToNumber(StringBuffer token_str) {
      String num = token_str.toString();
      switch (getNumType(num)) {
        case Hex:
          return StoneObject.alloc(Long.parseLong(num.substring(2), 16));
        case Int:
          return StoneObject.alloc(Integer.parseInt(num));
        case Long:
          return StoneObject.alloc(Long.parseLong(num));
        case Float:
          return StoneObject.alloc(Double.parseDouble(num));
      }
      return StoneObject.Null;
    }

    private StoneObject tokenize() {
      if (index > length)
        return StoneObject.Null;

      while (index < length && Character.isWhitespace(current()))
        next();

      if (index == length)
        return StoneObject.Null;

      if (isSpecial(source.charAt(index))) {
        next();
        return StoneObject.alloc(source.charAt(index - 1));
      }

      StringBuffer token_str;
      if (current() == '"') {
        token_str = new StringBuffer();
        next();

        while (current() != '"' && index < length) {
          token_str.append(current());
          next();
        }
        next();
        return new StoneObject.Symbol(token_str.toString());
      }

      token_str = new StringBuffer();

      while (index < (length -1) && !isDelimiter(current())) {
        token_str.append(current());
        next();
      }

      if (!isDelimiter(current())) {
        token_str.append(current());
        next();
      }

      StoneObject num;
      if ((num = convertToNumber(token_str)) == StoneObject.Null)
        return new StoneList.String(token_str.toString());
      return num;
    }
  }

  public static final StoneObject CloseParen = StoneObject.alloc(')');
  public static final StoneObject OpenParen = StoneObject.alloc('(');
  public static final StoneObject OpenBracket = StoneObject.alloc('[');
  public static final StoneObject CloseBracket = StoneObject.alloc(']');
  public static final StoneObject Dot = StoneObject.alloc('.');

  public static class PrimitiveFunction extends StoneObject {
    public PrimitiveFunction(int funcNum) {
      super(Type.Int.asScalar());
      putInt(funcNum);
    }

    public int arity() {
      throw new LangException("not implemented");
    }

    public StoneObject eval(Environment env, StoneObject args) {
      new Throwable().printStackTrace();
      throw new LangException("not implemented");
    }

    protected void validateNonNull(StoneList args) {
      if (args == null || args == StoneObject.Null)
        throw new LangException("monadic");
    }

    protected void validateMonadic(StoneList args) {
      if (args == null || args.length() != 1) 
        throw new LangException("monadic");
    }

    protected void validateMonadicDyadic(StoneList args) {
      if (args == null || args.length() <1 || args.length() >2) 
        throw new LangException("valence");
    }

    protected void validateDyadic(StoneList args) {
      if (args == null || args.length() < 2 || args.length()>2) 
        throw new LangException("dyadic");
    }

   protected void validateTriple(StoneList args) {
      if (args == null || args.length() < 3 || args.length()>3) 
        throw new LangException("dyadic");
    }

    protected StoneObject evalObject(Environment env, StoneObject obj) {
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
      } catch (LangException e) {
         env.debugError(e);
      }
      return ret;
    }
    
    protected StoneObject evalFirst(Environment env, StoneList args) {
      StoneObject head = args.first();
      StoneObject arg1 = StoneObject.Null;
      try {
        if (head.isList())  {
          if (StoneObject.Type.getType(head.getType()) == StoneObject.Type.Char) {
            arg1 = env.get(head);
            arg1.retain();
          } else 
            arg1 = head.eval(env, null);
        }
        else {
          arg1 = head.eval();
        }
      } catch (LangException e) {
        head.release();
        throw e;
      }
      head.release();
      return arg1;
    }

    protected StoneObject evalRest(Environment env, StoneList args) {
      StoneList tail = (StoneList)args.rest();
      StoneObject so = tail.getObject(0);
      StoneObject arg2 = StoneObject.Null;
      try {
        if (so.isList())
          arg2 = so.eval(env, null); 
        else 
          arg2 = so.eval();
      } catch (LangException e) {}
      tail.release();
      return arg2;
    }

  }

  public class Eq extends PrimitiveFunction {

    public Eq() {
      super(1);
    }

    public int arity() {
      return 2;
    }

    public StoneObject eval(Environment env, StoneList args) {
      validateDyadic(args);
      StoneObject ret = StoneObject.False;
      StoneObject arg1 = StoneObject.Null;
      StoneObject arg2 = StoneObject.Null;
      try {
        arg1 = evalFirst(env, args);
        arg2 = evalRest(env, args);
        if (arg1.equals(arg2))
          ret = StoneObject.True;
      } catch (LangException e) {
        env.debugError(e);
      }
      arg1.release();
      arg2.release();
      return ret;
    }
  }

  public class Quote extends PrimitiveFunction {

    public Quote() {
      super(2);
    }

    public int arity() {
      return 1;
    }

    public StoneObject eval(Environment env, StoneList args) {
      args.retain();
      return args;
    }
  }

  public class First extends PrimitiveFunction {
    public First() {
      super(3);
    }

    public int arity() {
      return 1;
    }

    public StoneObject eval(Environment env, StoneList args) {
      validateMonadic(args);
      StoneObject ret = StoneObject.Null;
      StoneObject arg1 = StoneObject.Null;
      try {
        arg1 = evalFirst(env, args);
        ret = arg1.first();
      } catch (LangException e) {
        env.debugError(e);
      }
      arg1.release();
      return ret;
    }
  }

  public class Rest extends PrimitiveFunction {
    public Rest() {
      super(4);
    }

    public int arity() {
      return 1;
    }

    public StoneObject eval(Environment env, StoneList args) {
      validateMonadic(args);
      StoneObject ret = StoneObject.Null;
      StoneObject arg1 = StoneObject.Null;
      try {
        arg1 = evalFirst(env, args);
        ret = arg1.rest();
      } catch (LangException e) {
        env.debugError(e);
      }
      arg1.release();
      return ret;
    }
  }

  public class Atoms extends PrimitiveFunction {

    public Atoms() {
      super(5);
    }

    public int arity() {
      return 1;
    }

    public StoneObject eval(Environment env, StoneList args) {
      if (args.length() == 1) 
        return StoneObject.True;
      return StoneObject.False;
    }
  }

  public class Cond extends PrimitiveFunction {

    public Cond() {
      super(6);
    }

    public int arity() {
      return 2;
    }

    public StoneObject eval(Environment env, StoneList args) {
      throw new LangException("not implemented");
    }
  }

  public class Join extends PrimitiveFunction {

    public Join() {
      super(7);
    }

    public int arity() {
      return 2;
    }

    public StoneObject eval(Environment env, StoneList args) {
      throw new LangException("not implemented");
    }
  }

  public class Plus extends PrimitiveFunction {

    public Plus() {
      super(8);
    }

    public int arity() {
      return 2;
    }

    public StoneObject eval(Environment env, StoneList args) {
      validateDyadic(args);
      StoneObject ret = StoneObject.Null;
      StoneObject arg1 = StoneObject.Null;
      StoneObject arg2 = StoneObject.Null;
      try {
        arg1 = evalFirst(env, args);
        arg2 = evalRest(env, args);
        if (arg1.isList()) {
          ret = arg1.add(arg2);
        } else if (arg2.isList()) {
          ret = arg2.add(arg1);
        } else 
          ret = arg1.add(arg2);
      } catch (LangException e) {
        env.debugError(e);
      }
      arg1.release();
      arg2.release();
      return ret;
    }
  }

  public class Minus extends PrimitiveFunction {

    public Minus() {
      super(9);
    }

    public int arity() {
      return 2;
    }

    public StoneObject eval(Environment env, StoneList args) {
      validateDyadic(args);
      StoneObject ret = StoneObject.Null;
      StoneObject arg1 = StoneObject.Null;
      StoneObject arg2 = StoneObject.Null;
      try {
        arg1 = evalFirst(env, args);
        arg2 = evalRest(env, args);
        ret = arg1.minus(arg2);
      } catch (LangException e) {
        env.debugError(e);
      }
      arg1.release();
      arg2.release();
      return ret;
    }
  }

  public class Setq extends PrimitiveFunction {
    public Setq() {
      super(10);
    }

    public StoneObject eval(Environment env, StoneList args) {
      validateDyadic(args);
      StoneObject ret = StoneObject.Null;
      StoneObject arg1 = StoneObject.Null;
      StoneObject arg2 = StoneObject.Null;
      try {
        arg1 = args.first();
        if (StoneObject.Type.getType(arg1.getType()) != StoneObject.Type.Char)
          throw new LangException("type: " + arg1);
        arg2 = evalRest(env, args);
        env.set(arg1, arg2);
        ret = StoneObject.True;
      } catch (LangException e) {
        env.debugError(e);
      }
      arg1.release();
      arg2.release();
      return ret;
    }

  }

  public class Def extends PrimitiveFunction {
    public Def() {
      super(50);
    }

    public StoneObject eval(Environment env, StoneList args) {
      StoneObject ret = StoneObject.Null;
      StoneObject func = StoneObject.Null;
      StoneObject param = StoneObject.Null;
      StoneObject body = StoneObject.Null;
      try {
        func = args.getObject(0);
        if (func.getType() != StoneObject.Type.Char.asList())
          throw new LangException("func: " + func);
        if (args.length() == 3) {
          param = args.getObject(1);
          body = args.getObject(2);
        } else if (args.length() == 2) {
          body = args.getObject(1);
        } else 
          throw new LangException("func: " + func);
        env.set(func, new StoneList.Lambda((StoneList)param, (StoneList)body));
        ret = StoneObject.True;
      } catch (LangException e) {
        env.debugError(e);
      }
      return ret;
    }

  }

  public class Println extends PrimitiveFunction {
    public Println() {
      super(11);
    }
    public StoneObject eval(Environment env, StoneList args) {
      validateMonadic(args);
      StoneObject ret = StoneObject.Null;
      try {
        ret = args.eval(env, null);
        StoneLisp.this.out.println(ret); 
        ret.release();
        ret = StoneObject.True;
      } catch (LangException e) {
        env.debugError(e);
      }
      return ret;
    }
  }

  public class Print extends PrimitiveFunction {
    public Print() {
      super(12);
    }

    public StoneObject eval(Environment env, StoneList args) {
      validateMonadic(args);
      StoneObject ret = StoneObject.Null;
      try {
        ret = args.eval(env, null);
        StoneLisp.this.out.print(ret); 
        ret.release();
        ret = StoneObject.True;
      } catch (LangException e) {
        env.debugError(e);
      }
      return ret;
    }
  }

  public static class Lambda extends PrimitiveFunction {
    StoneObject params;
    StoneList body;

    public Lambda(StoneObject params, StoneObject body) {
      super(1000);
      this.params = params;
      params.retain();
      this.body = (StoneList)body;
      body.retain();
    }

    public Environment push_bindings(Environment env, StoneObject values) {
      if (values == StoneObject.Null && params != StoneObject.Null)
        throw new LangException("params unbound " + "values: " + values + " params: " + params);
      
      env = env.push();
      if (params.length() != values.length()) {
        if (params.length() == 1 && values.length() == 0) 
          env.set(((StoneList)params).getObject(0), values);
        else 
          throw new LangException("params unbalanced");
      } else {
        for (long i = 0; i < params.length(); i++) {
          StoneList plist = (StoneList)params;
          StoneList vlist = (StoneList)values;
          env.set(plist.getObject(i), vlist.getObject(i));
        }
      }
      return env;
    }

    public void set_bindings(Environment env, StoneObject values) {

    }

    public StoneObject eval(Environment env, StoneList args) {
      Environment fenv = push_bindings(env, args);
      StoneObject ret = StoneObject.Null;
      try {
        ret = body.eval(fenv, null);
      } catch (LangException e) {
        fenv.debugError(e);
      }
      fenv.release();
      return ret;
    }

    public void release() {
      if (params != null) {
        params.release();
        params = null;
      }
      if (body != null) {
        body.release();
        body = null;
      }
      super.release();
    }

  }

  public class Distinct extends PrimitiveFunction {
    public Distinct() {
      super(101);
    }

    public StoneObject eval(Environment env, StoneList args) {
      validateMonadic(args);
        
      StoneObject ret = StoneObject.Null;
      try {
        StoneObject so = args.getObject(0);
        StoneList list = StoneList.Empty;
        if (so instanceof StoneList.String) {
          so = StoneFuncs.evalObject(env, so);
          if (so instanceof StoneList) 
            list = StoneFuncs.distinct((StoneList)so);
          so.release();
        } else if (so instanceof StoneList)
          list = StoneFuncs.distinct((StoneList)so);
        ret = list;
      } catch (LangException e) {
        env.debugError(e);
      }
      return ret;
    }
  }

  public class Table extends PrimitiveFunction {
    public Table() {
      super(101);
    }

    public StoneObject eval(Environment env, StoneList args) {
      validateNonNull(args);
        
      StoneObject ret = StoneObject.Null;
      StoneList rest = null;
      StoneList schema = null;
      try {
        StoneObject.Symbol tbl_name = (StoneObject.Symbol)args.getObject(0);
        rest = (StoneList)args.rest();
        schema = (StoneList)rest.eval(env, null);
        ret = new StoneTable(tbl_name, schema);
      } catch (LangException e) {
        env.debugError(e);
      }
      if (rest != null)
        rest.release();
      if (schema != null)
        schema.release();
      return ret;
    }
  }

  public class Insert extends PrimitiveFunction {
    public Insert() {
      super(102);
    }
    
    private long insert(StoneTable t, StoneList rowData) {
      int width = (int)rowData.length();
      if (width == 0) 
        return 0;
      long height = rowData.getObject(0).length();
      for (long i = 1; i < width; i++) 
        if (height != rowData.getObject(i).length())
          throw new LangException("length");
      if (height == 0) {
        StoneObject[] cols = new StoneObject[width];
        for (int i = 0; i < width; i++) 
          cols[i] = rowData.getObject(i);
        t.append(cols);
        return t.length();
      } else 
        return t.insert(rowData);
    }

    public StoneObject eval(Environment env, StoneList args) {
      validateNonNull(args);
        
      StoneObject ret = StoneObject.Null;
      StoneObject t = StoneObject.Null;
      StoneObject row = StoneObject.Null;
      try {
        t = args.getObject(0);
        t = env.get(t);
        if (t instanceof StoneTable) {
          StoneTable tbl = (StoneTable)t;
          row = evalObject(env, args.getObject(1));
          ret = StoneObject.alloc(insert(tbl, (StoneList)row));
        } else {
          throw new LangException("type");
        }
      } catch (LangException e) {
        env.debugError(e);
      }
      row.release();
      return ret;
    }
  }


  public class Upsert extends PrimitiveFunction {
    public Upsert() {
      super(1021);
    }
    
    private long upsert(StoneTable t, StoneList rowData) {
      int width = (int)rowData.length();
      if (width == 0) 
        return 0;
      long height = rowData.getObject(0).length();
      for (long i = 1; i < width; i++) 
        if (height != rowData.getObject(i).length())
          throw new LangException("length");
      if (height == 0) {
        StoneObject[] cols = new StoneObject[width];
        for (int i = 0; i < width; i++) 
          cols[i] = rowData.getObject(i);
        t.append(cols);
        return t.length();
      } else 
        return t.upsert(rowData);
    }

    public StoneObject eval(Environment env, StoneList args) {
      validateNonNull(args);
        
      StoneObject ret = StoneObject.Null;
      StoneObject t = StoneObject.Null;
      StoneObject row = StoneObject.Null;
      try {
        t = args.getObject(0);
        t = env.get(t);
        if (t instanceof StoneTable) {
          StoneTable tbl = (StoneTable)t;
          row = evalObject(env, args.getObject(1));
          ret = StoneObject.alloc(upsert(tbl, (StoneList)row));
        } else {
          throw new LangException("type");
        }
      } catch (LangException e) {
        env.debugError(e);
      }
      row.release();
      return ret;
    }
  }

  public class PlusUpsert extends PrimitiveFunction {
    public PlusUpsert() {
      super(1022);
    }
    
    private long plus_upsert(StoneTable t, StoneList rowData) {
      int width = (int)rowData.length();
      if (width == 0) 
        return 0;
      long height = rowData.getObject(0).length();
      for (long i = 1; i < width; i++) 
        if (height != rowData.getObject(i).length())
          throw new LangException("length");
      if (height == 0) {
        StoneObject[] cols = new StoneObject[width];
        for (int i = 0; i < width; i++) 
          cols[i] = rowData.getObject(i);
        return t.plus_upsert(cols);
      } else 
        return t.plus_upsert(rowData);
    }

    public StoneObject eval(Environment env, StoneList args) {
      validateNonNull(args);
        
      StoneObject ret = StoneObject.Null;
      StoneObject t = StoneObject.Null;
      StoneObject row = StoneObject.Null;
      try {
        t = args.getObject(0);
        t = env.get(t);
        if (t instanceof StoneTable) {
          StoneTable tbl = (StoneTable)t;
          row = evalObject(env, args.getObject(1));
          ret = StoneObject.alloc(plus_upsert(tbl, (StoneList)row));
        } else {
          throw new LangException("type");
        }
      } catch (LangException e) {
        env.debugError(e);
      }
      row.release();
      return ret;
    }
  }

  public class Rand extends PrimitiveFunction {
    public Rand() {
      super(103);
    }

    StoneList rand(StoneObject len, StoneObject upper) {
      byte t = upper.getType();
      long n = len.asLong();
      StoneList nums = null;
      long seed = System.nanoTime();
      if (t == StoneObject.Type.Int.asScalar()) {
        int up = upper.getInt();
        nums = new StoneList.Int(n, 0);
        for (long i = 0; i < n; i++) {
          seed = StoneUtil.randomLong(seed);
          nums.append(StoneUtil.long2int(seed)%up);
        }
      } else if  (t == StoneObject.Type.Long.asScalar()) {
        long up = upper.getLong();
        nums = new StoneList.Long(n, 0);
        for (long i = 0; i < n; i++) {
          seed = StoneUtil.randomLong(seed);
          nums.append(Math.abs(seed)%up);
        }
      } else if (t == StoneObject.Type.Double.asScalar()) {
        double up = upper.getDouble();
        nums = new StoneList.Double(n, 0);
        for (long i = 0; i < n; i++) {
          seed = StoneUtil.randomLong(seed);
          double unit = Math.abs(((seed<<32) + StoneUtil.randomLong(seed))/(double)(1L<<63));
          nums.append(up*unit);
        }
      } else {
        throw new SException("wrong type");
      }
      return nums;
    }

    public StoneObject eval(Environment env, StoneList args) {
      validateDyadic(args);
        
      StoneObject ret = StoneObject.Null;
      StoneObject n = StoneObject.Null;
      StoneObject upper = StoneObject.Null;
      try {
        n = evalObject(env, args.getObject(0));
        upper = evalObject(env, args.getObject(1));
        ret = rand(n, upper);
      } catch (LangException e) {
        env.debugError(e);
      }
      n.release(); 
      upper.release();
      return ret;
    }
  }

  public class Count extends PrimitiveFunction {
    public Count() {
      super(104);
    }

    public StoneObject eval(Environment env, StoneList args) {
      validateMonadic(args);
      StoneObject ret = StoneObject.alloc(0L);
      try {
        StoneObject so = evalObject(env, args.getObject(0));
        ret.putLong(so.length());
      } catch (LangException e) {
        env.debugError(e);
      }
      return ret;
    }
  }  

  public class SymEnum extends PrimitiveFunction {
    public SymEnum() {
      super(105);
    }

    public StoneObject eval(Environment env, StoneList args) {
      validateMonadicDyadic(args);
      StoneObject ret = StoneObject.Null;
      StoneList domain = StoneList.Empty;
      StoneList data = StoneList.Empty;
      try {
        domain = (StoneList)evalObject(env, args.getObject(0));
        if (args.length() == 2)
          data = (StoneList)evalObject(env, args.getObject(1));
        if (data != StoneList.Empty) 
          ret = new StoneEnum(domain, data);
        else 
          ret = new StoneEnum(domain);
      } catch (LangException e) {
        env.debugError(e);
      }
      domain.release();
      data.release();
      return ret;
    }
  }  

  public class Index extends PrimitiveFunction {
    public Index() {
      super(106);
    }

    private StoneList.Long toLong(StoneObject so) {
      if (!(so instanceof StoneList))
        throw new LangException("type");
      StoneList list = (StoneList)so; 
      StoneList.Long index = new StoneList.Long(list.length());
      for (long i = 0; i < list.length(); i++) {
        index.put(i, list.getObject(i).asLong());
      }
      return index;
    }

    public StoneObject eval(Environment env, StoneList args) {
      validateDyadic(args);
      StoneObject ret = StoneObject.Null;
      StoneObject so = StoneObject.Null;
      StoneObject index = StoneObject.Null;
      try {
        so = evalObject(env, args.getObject(0));
        index = evalObject(env, args.getObject(1));
        if (index instanceof StoneList.Long) {
          StoneList.Long idx = (StoneList.Long)index;
          ret = so.getList(idx);
        } else if (index instanceof StoneList.Int) {
          StoneList.Int idx = (StoneList.Int)index;
          ret = so.getList(idx);
        } else {
          StoneList.Long idx = toLong(index);
          ret = so.getList(idx);
          idx.release();
        }
      }  catch (LangException e) {
        env.debugError(e);
      }
      so.release(); 
      index.release();
      return ret;
    }
  }

  public class Dict extends PrimitiveFunction {
    public Dict() {
      super(107);
    }

    public StoneObject eval(Environment env, StoneList args) {
      validateDyadic(args);

      StoneObject ret = StoneObject.Null;
      StoneList keys = StoneList.Empty;
      StoneList values = StoneList.Empty;
      StoneObject key = StoneObject.Null;
      StoneList.Dictionary dict = null;
      try {
        StoneObject so = StoneFuncs.evalObject(env, args.getObject(0));
        if (so instanceof StoneList) 
          keys = (StoneList)so;
        else 
          key = so;
        so = StoneFuncs.evalObject(env, args.getObject(1));
        if (so instanceof StoneList)
          values = (StoneList)so;
        dict = new StoneList.Dictionary();
        if (key.isNull()) 
          dict.append(keys, values); 
        else 
          dict.append(key, values);
        ret = dict;
      } catch (LangException e) {
        if (dict != null) 
          dict.release();
        env.debugError(e);
      }
      keys.release();
      values.release();
      key.release();
      return ret;
    }
  }

  public class Send extends PrimitiveFunction {
    public Send() {
      super(108);
    }
  }

  public class SendRecv extends PrimitiveFunction {
    public SendRecv() {
      super(109);
    }
  }

  public class Handle extends PrimitiveFunction {
    public Handle() {
      super(109);
    }

   public StoneObject eval(Environment env, StoneList args) {
     validateDyadic(args);
     StoneObject ret = StoneObject.Null;
     StoneObject hostN = StoneObject.Null;
     StoneObject port = StoneObject.Null;
     try {
       hostN = StoneFuncs.evalObject(env, args.getObject(0));
       port = StoneFuncs.evalObject(env, args.getObject(1));
       if (hostN.getType() != StoneObject.Type.Symbol.asScalar() && port.getType() != StoneObject.Type.Int.asScalar())
         throw new LangException("type");
       ret = new StoneSocket.Client(hostN.toString(), port.getInt());
     } catch (LangException e) {
       env.debugError(e);
     }
     hostN.release(); 
     port.release();
     return ret;
   }
  }
  
  public class Closure extends Lambda {
    Environment env; 

    public Closure(Environment env, StoneList args, StoneObject body) {
      super(args, body);
      this.env = env;
    }

  }
}
