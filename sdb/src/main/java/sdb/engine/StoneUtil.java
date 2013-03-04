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

final class StoneUtil {

  final public static int long2int(final long l) {
    return (int)Math.abs((int)l);
  }

  public final static boolean isPowerOfTwo(int n) {
    return (n & -n) == n;
  }

  public final static boolean isPowerOfTwo(long n) {
    return (n & -n) == n;
  }

  final public static long randomLong(long x) {
    x ^= (x << 21); 
    x ^= (x >>> 35);
    x ^= (x << 4);
    return x;
  }
  
  final public static int nextPowerOfTwo(int x ) {
    if ( x == 0 ) return 1;
    x--;
    x |= x >> 1;
    x |= x >> 2;
    x |= x >> 4;
    x |= x >> 8;
    return ( x | x >> 16 ) + 1;
  }

  final public static long nextPowerOfTwo(long x ) {
    if ( x == 0 ) return 1;
    x--;
    x |= x >> 1;
    x |= x >> 2;
    x |= x >> 4;
    x |= x >> 8;
    x |= x >> 16;
    return ( x | x >> 32 ) + 1;
  }

  final public static int double2int(final double d ) {
    final long l = Double.doubleToRawLongBits( d );
    return (int)( l ^ ( l >>> 32 ) );
  }


  final public static int float2int(final float f ) {
    return Float.floatToRawIntBits( f );
  }

  final public static long murmurHash3(long x ) {
    x ^= x >>> 33;
    x *= 0xff51afd7ed558ccdL;
    x ^= x >>> 33;
    x *= 0xc4ceb9fe1a85ec53L;
    x ^= x >>> 33;

    return x;
  }

  final public static int murmurHash3(int x ) {
    x ^= x >>> 16;
    x *= 0x85ebca6b;
    x ^= x >>> 13;
    x *= 0xc2b2ae35;
    x ^= x >>> 16;
    return x;
  }
}
