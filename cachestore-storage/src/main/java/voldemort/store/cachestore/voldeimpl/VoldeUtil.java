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

package voldemort.store.cachestore.voldeimpl;

import voldemort.store.cachestore.Key;
import voldemort.store.cachestore.StoreException;
import voldemort.store.cachestore.Value;

import java.io.File;
import java.io.UnsupportedEncodingException;

import static voldemort.store.cachestore.BlockUtil.*;

/**
 * Created by IntelliJ IDEA.
 * User: mhsieh
 * Date: 4/13/12
 * Time: 10:07 AM
 * To change this template use File | Settings | File Templates.
 */
public class VoldeUtil {
//    public static final Key INDEX = Key.createKey("index");
//    public static final Key LAST_INDEX = Key.createKey("lastIndex");
//    public static final Key LAST_REC = Key.createKey("lastRec");
    public static int MAX_RECORD = 1000*1000;
    public static final String META_PREFIX = ".meta";
    public static final String POSTFIX = ".index";

    public static int toInt(Value<byte[]> value) {
        //Key key = toKey( value.getData());
        //return (Integer) key.getKey();
        byte[] data = value.getData();
        int len = getLen(data[0]);
        return readInt( data, len);

    }

    public static Value toValue(int no, long version) {
        byte[] bytes = toKeyBytes(Key.createKey( no));
        return new BlockValue<byte[]>(bytes, version, (short) 0);
    }


    public static long toLong(Value<byte[]> value) {
        //Key key = toKey( value.getData());
        //return (Long) key.getKey();
        byte[] data = value.getData();
        int len = getLen(data[0]);
        return readLong(data, len);

    }

    public static Value toValue(long no, long version) {
        byte[] bytes = toKeyBytes(Key.createKey(no));
        return new BlockValue<byte[]>(bytes, version, (short) 0);
    }

    public static short getShort(byte[] b, int off) {
	return (short) (((b[off + 1] & 0xFF) << 0) +
			((b[off + 0]) << 8));
    }

    public static String toStr(Value<byte[]> value) {
        //Key key = toKey(value.getData());
        //return  (String) key.getKey();
        byte[] data = value.getData();
        int len = getShort(data, 1);
        try {
            return new String(data, 3, len, "UTF-8" );
        } catch (UnsupportedEncodingException ex) {
            throw new StoreException(ex.getMessage(), ex);
        }
    }

    public static Value toValue(String str, long version) {
        byte[] bytes = toKeyBytes(Key.createKey( str));
        return new BlockValue<byte[]>(bytes, version, (short) 0);
    }

    public static String[] checkPath(String filename){
        String path = "./";
        String name ;
        int sep = filename.lastIndexOf( File.separator );
        if ( sep >= 0 ) {
            path = filename.substring(0, sep+1);
            name = filename.substring( sep+1, filename.length() );

        }
        else {
            int slash = filename.lastIndexOf("/");
            // no file seperator
            if (slash >= 0 ) {
                path = filename.substring(0, slash+1);
                name = filename.substring( slash+1, filename.length() );
            }
            else name = filename ;
        }
        //String[] toReturn = new String[]
        return new String[] { path, name};
    }

    public static void verifyPath(String path) {
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




}
