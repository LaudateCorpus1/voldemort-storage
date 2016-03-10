/*
 *
 * Copyright 2012-2015 Viant.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package voldemort.store.cachestore;

import voldemort.utils.ByteArray;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.Arrays;

import static voldemort.store.cachestore.BlockUtil.KeyType;

/**
 * Created by IntelliJ IDEA.
 * User: mhsieh
 * Date: 1/25/11
 * Time: 11:38 AM
 * Key support four different types - int, long, String, Bianry Array and Java Object
 *
 * To change this template use File | Settings | File Templates.
 */
public final class Key<T> implements Serializable, Comparable<T> {

    // generic type
    private T key;

    /**
     * @param key gerneral type
     */
    private Key(T key) {
        this.key =key;
    }


    public static Key createKey(int key) {
        return new Key( new Integer(key));
    }

    public static Key createKey(long key) {
        return new Key( new Long(key));
    }


    public static Key createKey(String key) {
        return new Key( key);
    }

    public static Key createKey(ByteArray array) {
        return new Key(array);
    }

    public static Key createKey(Object key) {
        if ( key instanceof Array) throw new StoreException("Array type is not supported");
        return new Key( key);
    }

    public static Key createKey(byte[] bytes) {
        return new Key( bytes);
    }

    public T getKey() {
        return key;
    }

    @Override
    public int hashCode() {
        if ( getKeyType (key) == KeyType.BYTEARY) {
            return Arrays.hashCode( (byte[]) key);
        }
        else
            return key.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj)
            return true;
        if ( obj instanceof Key ){
            if ( ((Key) obj).getKey() instanceof byte[] && key instanceof byte[])
                return isEqual( (byte[]) key , (byte[]) ((Key) obj).getKey() );
            else
                return key.equals(((Key) obj).getKey());
        }
        else
           return false;
    }

    private boolean isEqual(byte[] a, byte[] b) {
        if ( a.length == b.length) {
            for ( int i= 0; i < a.length; i++) {
                if ( a[i] != b[i])
                    return false;
            }
            return true;
        }
        else
            return false;
    }

    public BlockUtil.KeyType getType() {
        return getKeyType(key);
    }


    @Override
    public String toString() {
        return key.toString()+" "+ getKeyType(key);
    }

    public static KeyType getKeyType(Object object) {
        if ( object instanceof Integer) return KeyType.INT;
        else if ( object instanceof Long ) return KeyType.LONG;
        else if ( object instanceof String) return KeyType.STRING;
        else if ( object instanceof ByteArray) return KeyType.BARRAY;
        else if ( object instanceof byte[] ) return KeyType.BYTEARY;
        else  return KeyType.OBJECT;
    }

    /**
     * compare current only support int, long, String and ByteArray
     * anything else should use customize comparator
     * @param o
     * @return
     */
    @Override
    public int compareTo(T o) {
        if ( this == o ) return 0;
        if ( o instanceof Key) {
            // check key type
            Key oo = (Key) o;
            KeyType type = getType();
            if ( type != ((Key) o).getType()) return 0;
            switch ( type ) {
                case INT : return ((Integer) key).compareTo( (Integer) oo.getKey());
                case LONG : return ( (Long) key).compareTo( (Long) oo.getKey());
                case STRING : return ((String) key).compareTo( (String) oo.getKey());
                case BARRAY : return compareByteArray( (ByteArray) key, (ByteArray) oo.getKey());
                case BYTEARY: return compareArray( (byte[]) key,  (byte[]) oo.getKey() );
                default: return 1;
            }
        }
        else return 1;
    }

    private int compareArray(byte[] a, byte[] b) {
        int len = Math.max( a.length, b.length );
        for ( int i = 0; i < len ; i++ ) {
            if ( a.length >= i && b.length >= i) {
                if ( a[i] < b[i] ) return -1;
                else if ( a[i] > b[i] ) return 1;
            }
            else if ( a.length > i && b.length < i ) return 1;
            else if ( a.length < i && b.length > i ) return -1;
        }
        // same length and content
        return 0;
    }

    private int compareByteArray(ByteArray a, ByteArray b) {
        int len = Math.max( a.length(), b.length() );
        for ( int i = 0; i < len ; i++ ) {
            if ( a.length() >= i && b.length() >= i) {
                if ( a.get()[i] < b.get()[i] ) return -1;
                else if ( a.get()[i] > b.get()[i] ) return 1;
            }
            else if ( a.length() > i && b.length() < i ) return 1;
            else if ( a.length() < i && b.length() > i ) return -1;
        }
        // same length and content
        return 0;
    }
}

