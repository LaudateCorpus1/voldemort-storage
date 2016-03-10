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

package voldemort.store.cachestore.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import voldemort.store.cachestore.*;
import voldemort.store.cachestore.voldeimpl.KeyValue;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static voldemort.store.cachestore.impl.ChannelStore.open;

/**
 * Created by IntelliJ IDEA.
 * User: mhsieh
 * Date: 5/2/12
 * Time: 2:35 PM
 * To change this template use File | Settings | File Templates.
 */
public class SortedCacheStore extends CacheStore {
    private static Log logger = LogFactory.getLog(SortedCacheStore.class);
    // prefix + store
    public static final String META_DATA = ".meta.";
    protected ChannelStore metaDataStore ;
    protected Map<Key, CacheBlock<byte[]>> metaDataMap;

    public SortedCacheStore(String path){
        this( path, null);
    }

    public SortedCacheStore(String path, BlockSize blockSize) {
        this( path, blockSize, 0);
    }

    public SortedCacheStore(String path, BlockSize blockSize, int curIndex) {
        this(path, blockSize, curIndex, FILENAME );
    }
    /**
     * default delayWrite is false
     * @param path - data directory
     * @param blockSize - user defined interface
     * @param curIndex  - channel index
     * @param filename  - file name or store name
     */
    public SortedCacheStore(String path, BlockSize blockSize, int curIndex, String filename) {
        this(path, blockSize, curIndex, filename, false);
    }

    public SortedCacheStore(String path, BlockSize blockSize, int curIndex, String filename, boolean delayWrite) {
        // default mode =0
        this(path, blockSize, curIndex, filename, delayWrite, 0);
    }

    /**
     *
     * @param path
     * @param blockSize
     * @param curIndex
     * @param filename
     * @param delayWrite
     * @param mode
     */
    public SortedCacheStore(String path, BlockSize blockSize, int curIndex, String filename, boolean delayWrite, int mode) {
        this.delayWrite = delayWrite;
        if ( delayWrite ) {
            delayWriteQueue = new LinkedBlockingQueue<DelayBlock>(QUEUE_SIZE);
        }
        this.mode = mode;
        checkPath( path);
        this.path = path;
        this.blockSize = blockSize;
        this.curIndex = curIndex;
        this.overflow = new AtomicLong(0);
        this.list = new CopyOnWriteArrayList<ChannelStore>();
        this.namePrefix = filename;
        this.map = new ConcurrentSkipListMap<Key, CacheBlock>();
        //check to see if more file 0 / 1
        if ( curIndex  <= 1 ) {
            list.add(0, open( getPath(path) + filename+curIndex, map, false, 0, delayWrite, mode));
            deleted += list.get(0).getDeleted();
            if ( ChannelStore.isChannelExist(getPath(path) + filename + 1 +".ndx")) {
                // close channel 0
                list.get(0).close(delayWriteQueue);
                list.add(1, open( getPath(path) + filename + 1, map, false, 1, delayWrite, mode));
                this.curIndex = 1;
                deleted += list.get(1).getDeleted();
            }
        }
        else {
            throw new StoreException("not support for index "+ curIndex +" > 1 ");
        }
        initMetaDataStore();
        serverState = State.Active;
    }

    private ConcurrentSkipListMap getSkipListMap (Map map) {
        return (ConcurrentSkipListMap) map;
    }

    public ConcurrentSkipListMap getSkipListMap() {
        return (ConcurrentSkipListMap) map;
    }

    protected void initMetaDataStore() {
        metaDataMap = new ConcurrentHashMap<Key, CacheBlock<byte[]>> ();
        //
        metaDataStore = open(getPath(path) + META_DATA+ namePrefix, metaDataMap, false, 0, false, mode) ;
        //getSkipListMap(map).firstKey();
    }

    public final static int MAX_SIZE = 5000;
    public List<KeyValue> scan(Key from, Key to) {
        Key floor = findFloorKey(getSkipListMap(map), from);
        Key ceil = findCeilKey(getSkipListMap(map), to);
        ConcurrentNavigableMap<Key, CacheBlock> subMap = getSkipListMap(map).subMap(floor, true, ceil, true);
        if (subMap != null ) {
            int j = 0;
            List<KeyValue> list = new ArrayList<KeyValue>(subMap.size());
            Iterator<Key> keyIt= subMap.keySet().iterator();
            for( int i= 0; i < subMap.size() ; i++) {
                Key key = keyIt.next();
                if ( key != null && !skipKey(key, from, to) ) {
                    Value value = super.get(key);
                    if ( value != null ) {
                        list.add( new KeyValue( key, value));
                        j++;
                    }
                }
                if ( j > MAX_SIZE) {
                    logger.warn("exceed max list size "+MAX_SIZE);
                    break;
                }
            }
            return list;
        }
        return null;
    }


    public List<KeyValue> scan(Key from) {
        return scan (from, from);
    }

    public List<KeyValue> scan(List<Key> keyList) {
        List<KeyValue> list = new ArrayList<KeyValue>(keyList.size());
        for( Key key : keyList ) {
            Value value = super.get(key);
            if ( value != null ) {
                list.add( new KeyValue( key, value));
            }
        }
        return list;
    }

    // if key < from or key > to
    private boolean skipKey(Key key, Key from, Key to) {
        return  ((Comparable) from.getKey()).compareTo( (Comparable) key.getKey() ) > 0 ||
                 ((Comparable) to.getKey()).compareTo( (Comparable) key.getKey() ) < 0  ;
    }

    public Key findFloorKey(ConcurrentSkipListMap map, Key key){
        Key from = (Key) map.floorKey(key);
        if ( from == null )
            from = (Key) map.ceilingKey( key);
        return from;
    }

    public Key findCeilKey(ConcurrentSkipListMap map, Key key) {
        Key to = (Key) map.ceilingKey( key);
        if ( to == null )
            to = (Key) map.floorKey(key);
        return to;
    }

    public List<Boolean> multiPuts(List<KeyValue> keyValueList) {
        List<Boolean> list = new ArrayList<Boolean>();
        for (int i = 0; i < keyValueList.size() ; i++) {
            KeyValue kv = keyValueList.get( i);
            try {
                super.put( kv.getKey(), kv.getValue() );
            } catch (Exception ex) {
                list.add(false);
            }
        }
        return list;
    }




}
