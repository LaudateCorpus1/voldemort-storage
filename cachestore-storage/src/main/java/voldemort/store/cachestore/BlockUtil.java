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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import voldemort.utils.ByteArray;

import java.io.*;

/**
 * Created by IntelliJ IDEA.
 * User: mhsieh
 * Date: 1/25/11
 * Time: 11:31 AM
 * To change this template use File | Settings | File Templates.
 */
public final class BlockUtil {
    private static Log logger = LogFactory.getLog(BlockUtil.class);

    public static enum KeyType { INT ((byte) 0), LONG ((byte) 1), STRING((byte) 2), OBJECT((byte) 3), BARRAY( (byte)4 ),
            BYTEARY((byte) 5);

        final byte value ;

        KeyType(byte value) {
            this.value = value ;
        }

        public static KeyType getType(byte value) {
            switch (value) {
                case 0 : return INT;
                case 1 : return LONG;
                case 2 : return STRING;
                case 3 : return OBJECT;
                case 4 : return BARRAY;
                case 5 : return BYTEARY;
                default : throw new StoreException("Unknown type "+value) ;
            }
        }
    }

    // status + keyOffset2Len, dataOffset2Len, version2Record, short node
    public final static int RECORD_SIZE = 1 + 8 + 8 + 8 +2 ;
    // encodeing 44 bits for offset (max length 16 TB), 20 bits for size (1MB)
    public final static int LEN_OFFSET = 44 ;
    public final static int LEN = 20 ;
    // max file size 1TB
    public final static long MAX_FILE_SIZE  = 0x00000FFFFFFFFFFFL;
    // max block size 1 MB
    public final static int MAX_BLOCK_SIZE  = 0x000FFFFF ;
    // mask flag for last 20 bits
    public final static long LEN_MASK       = 0x00000000000FFFFFL;
    // mask flag for last 40 bits
    public final static long VERSION_MASK   = 0x00000FFFFFFFFFFFL;
    // delete flag on the last bit
    public final static byte DELETED = 0x01 ;
    // set undelete bits
    public final static byte UNDELETED = (byte) 0xFD;
    // status bit
    public final static int STATUS_BIT = 4;
    // isDirty bits 2nd bits
    public final static byte DIRTY = 0x02;
    // set for not dirty 2nd bits to 0
    public final static byte NOT_DIRTY = (byte) 0xFD;
    // non persist record record no
    public final static int INIT_RECORDNO = -1;


    /**
     * encode of offset (long) and length (int) into long, given the defined protocol
     * @param keyOffset ( the first 40 bits)
     * @param len ( the last 20b its)
     * @return encoding long
     */
    public static long convertOffset4Len(long keyOffset, int len) {
        return  ( keyOffset << LEN ) | len ;
    }

    /**
     * encoding version (long) and size (int) into long. given the defined protocol
     * @param version  ( the last 40 bits)
     * @param size ( the first 20 bits)
     * @return encoding long
     */
    public static long convertVersion4Size(long version, int size) {
        return ( (long) size << LEN_OFFSET) | version ;
    }

    /**
     * @param l -  encoding long
     * @return - return the size parts
     */
    public static long getOffset(long l) {
        return  l >>> LEN ;
    }

    public static int getLen(long l) {
        return (int) (l & LEN_MASK);
    }

    public static int getSize(long l) {
        return (int) (l >>> LEN_OFFSET) ;
    }

    public static long getVersionNo(long l) {
        return  (l & VERSION_MASK);
    }

    public static void checkFileSize(long len,  int size) {
        if ( len + size  > MAX_FILE_SIZE)
            throw new StoreException("Exceeding mae file size 1TB " + (len+size) );
    }

    /**
     *
     * @param i - int
     * @return number of condensed bytes
     */
    public static int getIntSize(int i) {
        if ( i < 0 ) return 4;
        else if ( i <= 0xFF ) return 1;
        else if ( i <= 0xFFFF) return 2;
        else if (i <= 0xFFFFFF) return 3;
        else return 4;
    }

    /**
     *
     * @param i - long
     * @return number of condensed byte
     */
    public static int getLongSize(long i) {
        if ( i < 0 ) return 8;
        else if ( i <= 0xFFL ) return 1;
        else if ( i <= 0xFFFFL) return 2;
        else if (i <= 0xFFFFFFL) return 3;
        else if (i <= 0xFFFFFFFFL) return 4;
        else if (i <= 0xFFFFFFFFFFL) return 5;
        else if (i <= 0xFFFFFFFFFFFFL) return 6;
        else if (i <= 0xFFFFFFFFFFFFFFL) return 7;
        else return 8;

    }

    // right most 4 bits for key type (0-15)
    // the rest of 4 bits for key length
    public static byte makeLen4Type(int size, int type) {
        return (byte) ( size << 4 | type );
    }

    public static void makeInt(byte[] bytes, int value, int size) {
        for( int i = 1; i <= size ; i++) {
            bytes[i] = (byte) (value >>> 8*(i-1));
        }
    }

    public static void makeLong(byte[] bytes, long value, int size) {
        for( int i = 1; i <= size ; i++) {
            bytes[i] = (byte) (value >>> 8*(i-1));
        }
    }


    public static byte[] toKeyBytes(Key key) {
        KeyType t = key.getType();
        byte[] bytes;
        int size;
        switch (t ) {
            case INT :
                int value = (Integer) key.getKey();
                size = getIntSize( value);
                bytes = new byte[ size + 1];
                bytes[0] = makeLen4Type(size, t.value );
                makeInt(bytes, value, size);
                return bytes;
            case LONG :
                long l = (Long) key.getKey();
                size = getLongSize( l);
                bytes = new byte[ size + 1];
                bytes[0] = makeLen4Type(size, t.value );
                makeLong(bytes, l, size);
                return bytes;
            case STRING :
                byte[] str;
                try {
                    str = ((String) key.getKey()).getBytes("UTF-8");
                } catch (UnsupportedEncodingException ex) {
                    throw new StoreException( ex.getMessage(), ex);
                }
                //key for string can not longer than 32K
                short len = (short) str.length;
                bytes = new byte[ 1+2+ str.length ];
                bytes[0] = t.value;
                Bits.putShort(bytes, 1, len);
                System.arraycopy(str, 0, bytes, 3, str.length);
                return bytes ;
            case BARRAY:
                ByteArray array = (ByteArray) key.getKey();
                bytes = new byte[ array.get().length + 1 ];
                bytes[0] = t.value;
                System.arraycopy(array.get(), 0, bytes, 1, bytes.length -1 );
                return bytes;
            case BYTEARY:
                byte[] bs = (byte[]) key.getKey();
                bytes = new byte[ bs.length +1 ];
                bytes[0] = t.value;
                System.arraycopy(bs, 0, bytes, 1, bytes.length -1 );
                return bytes;
            default:
                //@TODO support different serialize and deseiralze
                byte[] obj = toJBytes( key.getKey() );
                bytes = new byte[ 1+ obj.length ];
                bytes[0] = t.value;
                System.arraycopy(obj, 0, bytes, 1, obj.length);
                return bytes;
        }

    }

    public static KeyType getType(byte type) {
        byte t = (byte) ( type & 0x0F);
        return KeyType.getType( t);
    }

    public static int getLen(byte type) {
        return ( (type & 0xFF) >>> 4);
    }

    public static int readInt(byte[] data, int len) {
        int k = 0 ;
        for (int i=1 ; i <= len ; i++) {
            k += ( data[i] & 0xFF ) << 8 * (i-1) ;
        }
        return k;
    }

    public static long readLong(byte[] data, int len){
        long l =0;
        for ( int i = 1; i <= len ; i++) {
            l +=  (data[i] & 0xFFL  ) << 8 * (i-1) ;
        }
        return l;
    }

    public static Key toKey(byte[] data) {
        KeyType t = getType( data[0]);
        int len ;
        switch (t ) {
            case INT :
                len = getLen(data[0]);
                return Key.createKey( readInt( data, len));
            case LONG :
                len = getLen(data[0]);
                return Key.createKey( readLong( data, len));
            case STRING :
                len = Bits.getShort(data, 1);
                String s;
                try {
                    s = new String(data, 3, len, "UTF-8" );
                } catch (UnsupportedEncodingException ex) {
                    throw new StoreException(ex.getMessage(), ex);
                }
                return Key.createKey(s);
            case BARRAY:
                byte[] b = new byte[ data.length -1];
                System.arraycopy(data, 1, b, 0, b.length);
                return Key.createKey( new ByteArray( b));
            case BYTEARY:
                byte[] bs = new byte[ data.length -1];
                System.arraycopy(data, 1, bs, 0, bs.length);
                return Key.createKey( bs);
            default:
                return Key.createKey( toJObject(data,1 , data.length-1));
        }

    }

    public static byte[] toJBytes(Object object) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try{
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(object);
        }catch(java.io.IOException ioe){
            throw new RuntimeException( ioe.getMessage(), ioe );
        } finally {
            if (baos != null )
                try {
                    baos.close();
                } catch (IOException ex) {}
        }
        return baos.toByteArray();
    }

    public static Object toJObject(byte[] bytes){
        return toJObject(bytes, 0, bytes.length);
    }

    public static Object toJObject(byte[] bytes, int offset, int len){
        Object object = null;
        try{
            object = new ObjectInputStream(new ByteArrayInputStream(bytes, offset, len)).readObject();
        }catch(java.io.IOException ioe){
            throw new RuntimeException( ioe.getMessage(), ioe );
        }catch(java.lang.ClassNotFoundException ex){
            throw new RuntimeException( ex.getMessage(), ex );
        }
        return object;
    }

    public static int defineSize(int len) {
        if ( len <= 0x003FF )  // 1K , 25 %
            return len + len /4 ;
        else if (len <= 0x00FFF ) // 4K 20%
            return len + len /5 ;
        else if (len <= 0x08FFF ) // 32K 10%
            return len + len /10 ;
        else if (len <= 0xFFFF ) // 65K 6%
            return len + len /12 ;
        else if (len <= 0xFFFFF ) // 1MB 4%
            return len + len /25 ;
        else if (len > MAX_BLOCK_SIZE ) //
            throw new StoreException("Exceeding 1 MB len "+ len) ;
        else{
            int l =  len + len /50 ; // 2%
            if ( l >=  MAX_BLOCK_SIZE ) return  MAX_BLOCK_SIZE;
            else return l ;
        }
    }

    /**
     * copy the content of block from source to destination exception size2version info
     * @param src - source block
     * @param dst - destination block
     */
    public static void copyBlock(CacheBlock src, CacheBlock dst) {
        // copy everything except version
        dst.setStatus(src.getStatus() );
        dst.setBlockSize( src.getBlockSize() );
        dst.setDataOffset( src.getDataOffset());
        dst.setDataLen( src.getDataLen() );
    }

    /**
     * if value's version == 0, it will not perform checking, just increament current vrsion no
     * otherwise it value's version against block's version , if it is less than block's version
     * if it is greater than block's version, multiPuts block version to value's version
     *
     * throw a StoreException
     * @param block
     * @param value
     */
    public static void checkVersion(CacheBlock block, Value value) {
        if ( value.getVersion() > 0 ) {
            if ( value.getVersion() < block.getVersion() ) {
                logger.warn("Outdated version "+value.getVersion()+" current "+ block.getVersion() );
                return ;
            }
            if ( value.getVersion() > block.getVersion() + 1 )
                logger.warn("Version "+value.getVersion()+" > "+ block.getVersion());
        }
        // version is 0,  just using current version + 1, otherwise overwrite with version from value
        long version = value.getVersion() == 0 ?  block.getVersion()+1 : value.getVersion();
        logger.info(" value " + value.getVersion()+" block " + block.getVersion());
        block.setVersion( version);
    }


    public static byte[] longToBytes(long data){
        byte[] b = new byte[8];
        Bits.putLong( b, 0,  data);
        return b;
    }

    public static long bytesToLong( byte[] data){
        return Bits.getLong( data, 0 );
    }

     /**
       * make sure the path is exist, otherwise create it
       * @param path of data directory
       */
    public static void checkPath(String path) {
        File file = new File(path);
        if ( file.isDirectory()) {
          return ;
        }
        else {
          if ( ! file.exists() ) {
              if ( ! file.mkdirs() )
                  throw new RuntimeException("Fail to create path "+path);
          }
          else
              throw new RuntimeException(path+" is a file");
        }
    }

    public static boolean isDeleted(byte b) {
        if ( (b & DELETED) == DELETED ) return true;
        else return false;
    }
}

