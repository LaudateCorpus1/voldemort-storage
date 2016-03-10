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
import voldemort.versioning.ObsoleteVersionException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import static voldemort.store.cachestore.BlockUtil.*;
import static voldemort.store.cachestore.impl.CacheValue.createValue;
import static voldemort.store.cachestore.impl.ChannelStore.open;

/**
 * Created by IntelliJ IDEA.
 * User: thsieh
 * Date: May 15, 2010
 * Time: 2:17:38 PM
 * To change this template use File | Settings | File Templates.
 */

public class CacheStore {
    private static Log logger = LogFactory.getLog(CacheStore.class);
    // indicate server state
    public static enum State  { Active, Backup, Pack, Purge, Shutdown }

    protected ConcurrentMap<Key, CacheBlock> map;
    // use to protect channel object data offset, total record, key offset and log offset
    protected final ReentrantLock lock = new ReentrantLock();
    // interface to determine the size of block
    protected BlockSize blockSize;
    // list of channelStore which handler the persistence
    protected List<ChannelStore> list;
    protected static final String FILENAME = "cachesotre";
    // index which represent current
    protected volatile int curIndex ;
    // path for for data file
    protected String path;
    // record data block overflow
    protected AtomicLong overflow ;
    //private volatile long triggerSize= (1 << 32) ;
    // default as 256 MB
    protected volatile long trigger = ( 1 << 28 );
    // use to prevent more than two request for packing data
    protected final ReentrantLock packLock = new ReentrantLock();
    // write back flag
    protected boolean delayWrite;
    // # of delete record
    protected volatile long deleted;
    protected String namePrefix ;
    protected static final int QUEUE_SIZE = 200000;
    protected BlockingQueue<DelayBlock> delayWriteQueue;
    // server state
    protected volatile State serverState;
    // daemon thread for pack
    protected TimerTask packTask;
    // daemon thread for purge
    protected TimerTask purgeTask;
    // daemon thread for backup
    protected TimerTask backupTask;
    // keep track of timestamp for each operation, pack backup and purge
    protected List<Long> statList;
    protected AtomicLong cacheHit = new AtomicLong(0);
    protected AtomicLong cacheMis = new AtomicLong(0);
    // mode
    protected int mode;
    // cache memory size
    protected AtomicLong currentMemory = new AtomicLong(0);
    // cap
    protected volatile int maxCacheMemory ;
    //useMaxCache
    protected volatile boolean useMaxCache = false;
    // double linked list
    protected AccessQueue<Reference> doubleLinkedList;
    // action queue for doubleLinkedList
    protected LinkedBlockingQueue<String> actionQueue;
    // flag for lock type Access queue
    protected volatile boolean useNoLock = false;
    protected volatile boolean useLRU = false;
    public static final int NO_CACHE = 3;

    protected CacheStore(){
        super();
    }

    public CacheStore(String path){
        this( path, null);
    }

    /**
     * default delayWrite is false
     * @param path - data directory
     * @param blockSize - user defined interface
     * @param curIndex  - channel index
     * @param filename  - file name or store name
     */
    public CacheStore(String path, BlockSize blockSize, int curIndex, String filename) {
        this(path, blockSize, curIndex, filename, false);
    }

    public CacheStore(String path, BlockSize blockSize, int curIndex, String filename, boolean delayWrite) {
        // default mode =0
        this(path, blockSize, curIndex, filename, delayWrite, 0);
    }

    public CacheStore(String path, BlockSize blockSize, int curIndex, String filename, boolean delayWrite, int mode) {
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
        this.map = new ConcurrentHashMap( findMapSize( getPath(path)+ filename));
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
        //if ( delayWrite ) new WriteBackThread().start();
        serverState = State.Active;
        //init();
    }



    /**
     *
     */
    public void setMaxCacheMemory(long maxCache) {
        maxCacheMemory = (int) maxCache;
        logger.info("using property by max cache size "+ maxCache );
        doubleLinkedList = makeQueue() ;
        logger.info("useNoLock "+useNoLock +" "+doubleLinkedList.getClass().getName() );
        actionQueue = new LinkedBlockingQueue<String>(maxCacheMemory);
    }

    private AccessQueue makeQueue() {
        return  new DoubleLinkedList<Reference>(maxCacheMemory );
    }

    public void setUseNoLock(boolean useNoLock) {
        this.useNoLock = useNoLock;
    }

    public boolean isNoCache() {
        if ( !useMaxCache && mode >= NO_CACHE && mode < 2* NO_CACHE ) return true;
        else return false;
    }

    /**
     *
     * @return DrainQueueThread
     */
    public DrainQueueThread startDrainQueue(){
        useMaxCache =  true;
        DrainQueueThread dr= new DrainQueueThread();
        dr.start();
        return dr;
    }

    public List<WriteBackThread> startWriteThread(int no) {
        List<WriteBackThread> list = new ArrayList<WriteBackThread>();
        if ( no < 1 ) {
            logger.info("assign to write thread from "+no+" to 1");
            no =1;
        }
        logger.info("start WriteBackThread total "+no);
        for ( int i =0; i < no ; i++) {
            list.add(new WriteBackThread(i));
            list.get(i).start();
        }
        return list;
    }

    public void startPackThread(long period) {
        // check pack data damon thread  for each hour
        if ( period > 0) {
            logger.info("Start the pack data thread");
            packTask = new PackThread(this, false);
            new Timer().schedule(packTask, period, period);
        }
    }

    protected int findMapSize(String filename) {
        int total = 0 ;
        File file = new File(filename+"0.ndx");
        if ( file.exists() && file.length() > ChannelStore.OFFSET + RECORD_SIZE ) {
           total += (file.length() - ChannelStore.OFFSET ) /RECORD_SIZE ;
        }
        file = new File( filename+"1.ndx");
        if ( file.exists() && file.length() > ChannelStore.OFFSET + RECORD_SIZE ) {
           total += (file.length() - ChannelStore.OFFSET ) /RECORD_SIZE ;
        }
        return total > 1129 ? total : 1129 ;

    }

    /**
     * make sure the path is exist, otherwise create it
     * @param path of data directory
     */
    protected void checkPath(String path) {
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

    public CacheStore(String path, BlockSize blockSize, int curIndex) {
        this(path, blockSize, curIndex, FILENAME );
    }

    /**
     * set map size to reduce frequency dynamic rehash
     * @param size
     */
    public void resetMapSize(int size){
        if ( map.size() == 0 ) {
            logger.info("reset map size "+size);
            map = new ConcurrentHashMap( size);
        }
        else logger.warn("map size "+map.size()+" is not zero, reset fail!!!!");
    }

    public CacheStore(String path, BlockSize blockSize) {
        this( path, blockSize, 0);
    }


    public FileChannel cloneDataChannel(ChannelStore channelStore) throws FileNotFoundException {
        return  new RandomAccessFile(channelStore.getFilename()+".data", "rw").getChannel();
    }

    protected String getPath(String path) {
        if ( path.charAt( path.length()-1) == '/')
            return path ;
        else
            return path +"/" ;
    }

    public ChannelStore getChannel(CacheBlock block) {
        int i = block.getIndex() ;
        if ( i >= list.size() ) {
            //list.add(i, open( getPath(path) + FILENAME+i, map));
            throw new StoreException("Channel list out of bound size "+list.size()+" expect " +i);
        }
        return list.get(i);
    }

    public List<ChannelStore> getList() {
        return list;
    }


    public int getCurIndex() {
        return curIndex;
    }

    public List<Long> getStatList() {
        return statList;
    }

    public void setStatList(List<Long> statList) {
        this.statList = statList;
    }

    public void setCurIndex(int curIndex) {
        this.curIndex = curIndex;
    }

    public ConcurrentMap<Key, CacheBlock> getMap() {
        return map;
    }

    public ReentrantLock getLock() {
        return lock;
    }

    public String getStat() {
        long hit = cacheHit.get();
        long miss = cacheMis.get();
        ChannelStore channel = list.get( curIndex);
        return "Filename "+getNamePrefix()+curIndex +", total records " + channel.getTotalRecord() + ", deleted "+deleted
                +", active "+ (channel.getTotalRecord() - deleted )+ ", file size " + channel.getDataOffset()
                + ", block overflow " + overflow + ", purge trigger "+trigger +"\n cache hit "+ hit + " cache miss "+ miss
                + " ratio " + ( miss == 0 ? "100% " : ( hit * 100 / (hit+miss) )) ;
    }

    public BlockingQueue<DelayBlock> getDelayWriteQueue() {
        return delayWriteQueue;
    }

    public void forceFlush() {
        for ( int i =0 ; i < list.size() ; i ++ ) {
            logger.info("force flush "+list.get(i).getFilename() );
            list.get(i).forceFlush();
        }
    }

    public Value get(Key key) {
        // check shutdown
        if ( serverState == State.Shutdown ) {
            logger.warn("Server is in shutdown state "+key.toString() );
            throw new StoreException("Server is in shutdown state");
        }
        CacheBlock<byte[]> block = map.get( key);
        if ( block == null ) return null;
        else {
            CacheValue cacheValue = null;
            boolean add = true ;
            synchronized ( block) {
                if ( block.getData() == null ) {
                    try {
                        //byte[] data = readChannel( block.getDataOffset2Len(), dataChannel );
                        ChannelStore channel = getChannel(block);
                        byte[] data = channel.readChannel( block.getDataOffset2Len(), channel.getDataChannel() );
                        // check delay write
                        //block.setData(data);
                        createReference( block, data );
                        if (useMaxCache) currentMemory.getAndAdd( data.length );
                    } catch (IOException ex) {
                        logger.error( ex.getMessage()+" "+block.toString(), ex);
                        throw new StoreException(ex.getMessage());
                    }
                    cacheMis.getAndIncrement();
                }
                else {
                    add = false;
                    cacheHit.getAndIncrement();
                }
                cacheValue = createValue(block.getData(), block.getVersion() , block.getNode());
                if ( isNoCache() ) block.setValue( null);
                //return createValue(block.getData(), block.getVersion() , block.getNode()) ;
            }
            // must outside sync block
            if ( add ) add2Queue(block);
            else {
                needToMove( block);
            }
            return cacheValue;
        }
    }

    private void add2Queue(CacheBlock block, boolean flag) {
        if (useMaxCache && ! useNoLock ) {
            doubleLinkedList.add2Tail(block);
            if ( flag )
                needToDrain( block);
        }
    }

    private void add2Queue(CacheBlock block){
        add2Queue(block, true);

    }



    private void needToMove(CacheBlock block) {
        //check useLRU, default is no LRU
        if (useMaxCache && useLRU && block.getValue() != null ) {
            try {
                if( ! doubleLinkedList.move2Tail( block) ) {
                    logger.info("move fail # "+block.getRecordNo()+" "+checkNull(block.getValue() ));
                    add2Queue( block, false);
                 }
            } catch (RuntimeException rx) {
                // swallow exception
                logger.error(rx.getMessage(), rx);
            } finally {
                //return toReturn;
            }
        }
    }

    private String checkNull(Reference ref) {
        StringBuilder sb = new StringBuilder();
        if ( ref == null ) sb.append("ref null ");
        else {
            if ( ref.getNext() == null ) sb.append("next null ");
            if ( ref.getPrev() == null ) sb.append("prev null ");
            if ( ref.getData() == null ) sb.append("data null ");
            sb.append( ref.toString() );
        }
        return sb.toString();
    }

    private int findBlockSize(int len) {
        int size ;
        if ( this.blockSize != null ) {
            size = blockSize.defineSize(len);
        }
        else {
            size= defineSize( len);
            if ( size < len ) {
                logger.warn("size "+ size+" should not be less than "+len+"  use "+len);
                size = len;
            }
        }
        if ( size > MAX_BLOCK_SIZE ) {
            logger.error("defineSize " + size +" exceeding "+MAX_BLOCK_SIZE);
            throw new StoreException("defineSize " + size +" exceeding "+MAX_BLOCK_SIZE);
        }
        else return size ;
    }

    public boolean isDelayWrite() {
        return delayWrite;
    }



    public CacheBlock makeBlock(int record, int blockSize, int dataLen, long version, short node) {
        //long data = (dataOffset << LEN | dataLen) ;
        long data = convertOffset4Len(list.get(curIndex).getDataOffset(), dataLen) ;
        checkFileSize(list.get(curIndex).getDataOffset(), dataLen);
        long b2v = convertVersion4Size(version, blockSize) ;
        // ---must use setIndex --
        CacheBlock block = new CacheBlock( record, data, b2v, (byte) 0,  node );
        block.setIndex( curIndex);
        return block ;
    }


    private void checkVersion(CacheBlock block, Value value, Key key) {
        if ( value.getVersion() > 0 ) {
            if ( value.getVersion() <= block.getVersion() ){
                logger.warn("Obsolete "+value.getVersion()+" block "+ block.getVersion()+" rec "+block.getRecordNo()+" "+key.toString() );
                throw new ObsoleteVersionException("ObsoleteVersion "+value.getVersion()+" cur "+
                        block.getVersion()+" rec "+block.getRecordNo()+ " "+key.toString() );
            }
            else if ( value.getVersion() > block.getVersion()+ 1 )
                logger.warn("Version "+value.getVersion()+" > "+ block.getVersion()+" rec "+block.getRecordNo()+" "+key.toString());
        }
        // version is 0,  just using current version + 1, otherwise overwrite with version from value
        long version = value.getVersion() == 0 ?  block.getVersion()+1 : value.getVersion();
        block.setVersion( version);
    }

    private void copyBlock(CacheBlock src, CacheBlock dst) {
        // copy everything except key info
        dst.setStatus(src.getStatus() );
        dst.setBlockSize( src.getBlockSize() );
        dst.setDataOffset( src.getDataOffset());
        dst.setDataLen( src.getDataLen() );
        //dst.setNode(src.getNode());
        dst.setVersion( src.getVersion());
    }

    private long offerTime = 500 ;

    /**
     *
     * @param key
     * @param value - byte[]
     * @param inMemory - use to avoid write disk for every put
     */

    public void delayWrite(Key key, Value<byte[]> value, boolean inMemory) {
        CacheBlock<byte[]> block = map.get( key);
        if( block == null ) {
            int size = findBlockSize( value.getData().length );
            long dif = value.getData().length;
            // lock at channel store writeback routne at chnnale store
            //lock.lock();
            try {
                // for delay write, set record# -1for new , will multiPuts during Daemon thread
                block = makeBlock(INIT_RECORDNO, size, value.getData().length, 1L, value.getNode() );
                //DataOffset to 0, it is a new block, make use current offset, was overwriten
                block.setDataOffset( 0L);
                CacheBlock tmp = map.putIfAbsent(key , block);
                if ( tmp != null ){
                    checkVersion(tmp, value, key);
                    logger.error("tmp "+tmp.toString()+" block "+block.toString());
                    dif = block.getDataLen() - tmp.getDataLen() ;
                    // check if tmp's block size can hold data
                    if ( value.getData().length  > tmp.getBlockSize()) {
                        //copyBlock(tmp, block) ;
                        copyBlock( block, tmp);
                    }
                    else {
                        tmp.setDataLen( value.getData().length);
                    }
                    block = tmp ;
                }
                synchronized ( block) {
                    //block.setData(value.getData());
                    createReference( block, value.getData());
                    if (useMaxCache) currentMemory.getAndAdd( dif );
                    block.setDirty( true);
                    // don't write to disk if inMemory is true
                    if ( ! inMemory ) {
                        try {
                            boolean s = delayWriteQueue.offer( new DelayBlock(block, key), offerTime, TimeUnit.MILLISECONDS);
                            if ( ! s ) {
                                logger.error("Fail to offer size " + delayWriteQueue.remainingCapacity());
                                throw new StoreException("over capacity "+QUEUE_SIZE+ " time in ms "+offerTime);
                            }
                        } catch (InterruptedException e) {
                            logger.error(e.getMessage(), e);
                            throw new StoreException(e.getMessage(), e);
                        }
                    }
                }
                // move out sync block
                add2Queue( block);

            } finally {
                //lock.unlock();
            }
        } // block == null
        else {
            synchronized( block) {
                // double check , in case deleted
                if ( map.get(key) == null ) map.put(key, block) ;
                // only validate for version no exist, means > 0
                checkVersion( block, value, key);
                long dif = value.getData().length - block.getDataLen();
                block.setNode( value.getNode());
                block.setDataLen( value.getData().length );
                checkFileSize( block.getDataOffset() , value.getData().length );
                if ( value.getData().length > block.getBlockSize() ) {
                    int size = findBlockSize( value.getData().length );
                    block.setBlockSize( size);
                    // due to delay write, don't know DataOffset yet
                    block.setDataOffset(0L);
                }
                //block.setData(value.getData());
                updateReference( block, value.getData());
                if (useMaxCache) currentMemory.getAndAdd( dif );
                block.setDirty( true);
                if ( ! inMemory ) {
                    try {
                        boolean s = delayWriteQueue.offer( new DelayBlock(block, key), offerTime, TimeUnit.MILLISECONDS);
                        if ( ! s ) {
                            logger.error("Fail to offer size " + delayWriteQueue.remainingCapacity());
                            throw new StoreException("over capacity "+QUEUE_SIZE+ " time in ms "+offerTime);
                        }
                    } catch (InterruptedException e) {
                        logger.warn("intEx "+ e.getMessage(), e);
                        throw new StoreException(e.getMessage(), e);
                    }
                }
            }
        }

    }

    public void putInMap(Key key, Value<byte[]> value ) {
        //delaywrite must be true
        if ( ! delayWrite ) throw new RuntimeException("delayWrite must be true");
        delayWrite( key, value, true);
    }

    public void put(Key key, Value<byte[]> value) {
        //  shutdown
        if ( serverState == State.Shutdown ) {
            logger.warn("Server is in shutdown state "+key.toString() );
            throw new StoreException("Server is in shutdown state");
        }
        // if delayWrite go different routine
        if ( delayWrite )  {
            // inMemory is false
            delayWrite( key, value, false);
            return;
        }
        CacheBlock block = map.get( key);
        long keyOffset2Len = 0;
        if ( block == null ) {
            // write a new block
            ChannelStore channel = list.get(curIndex);
            int size = findBlockSize( value.getData().length );
            if ( size == 0 ) logger.warn("size = 0  key "+key.toString() );
            byte[] keyBytes = toKeyBytes(key);
            lock.lock();
            boolean add = true;
            try {
                block = makeBlock(channel.getTotalRecord(), size, value.getData().length, 1L , value.getNode());
                CacheBlock tmp = map.putIfAbsent(key , block);
                //concurrency issue compare version here, some thread get ahead after get, using
                //reassign block from tmp, no longer new block
                if ( tmp != null ){
                    checkVersion(tmp, value, key);
                    // check if tmp's block size can hold data
                    if ( tmp.getBlockSize() <  value.getData().length ) {
                        logger.info("copy block "+ tmp.getRecordNo()+" len "+block.getDataLen() );
                        //copyBlock(tmp, block) ;
                        //move data from block to tmp , since tmp had key written
                        add = false;
                        copyBlock(block, tmp);
                        // multiPuts offset
                        channel.setDataOffset( channel.getDataOffset()+ block.getBlockSize() ) ;
                        block = tmp;
                    }
                    else { //other wise add is false
                        add = false;
                        //change pointer
                        tmp.setDataLen( value.getData().length );
                        block = tmp ;
                    }
                }
                else { //add mode
                    keyOffset2Len = convertOffset4Len( channel.getKeyOffset(), keyBytes.length);
                    // multiPuts offset position
                    lock.lock();
                    try {
                        channel.setTotalRecord(channel.getTotalRecord()+1);
                        channel.setDataOffset( channel.getDataOffset() + size);
                        channel.setKeyOffset( channel.getKeyOffset() + keyBytes.length );
                    } finally {
                        lock.unlock();
                    }
                    //logger.info("offset "+ channel.getDataOffset()+" block "+block.toString());
                }
                // multiPuts node
                block.setNode( value.getNode());
                // multiPuts cache data
                //block.setData( value.getData());
                createReference( block, value.getData() );
                if (useMaxCache) currentMemory.getAndAdd( value.getData().length );
            } finally {
                lock.unlock();
                add2Queue( block);
            }
            try {
                //base on add mode
                if ( ! add  )
                    channel.writeExistBlock(block);
                else
                    channel.writeNewBlock(block, keyOffset2Len, keyBytes);
            } catch (IOException ex) {
                logger.error( ex.getMessage(), ex);
                throw new StoreException(ex.getMessage());
            } finally {
                if ( block != null && isNoCache()  ) {
                    synchronized (block) {
                        block.setValue( null);
                    }
                }
            }
        }
        else {  // block is not null
            //lock on block
            synchronized( block) {
                if ( block.getDataLen() == 0 ) logger.warn("data len 0 "+key.getKey().toString() );
                // double check , in case deleted
                if ( map.get(key) == null ) {
                    logger.info("put it again "+key.toString());
                    map.put(key, block) ;
                }
                ChannelStore channel = getChannel(block);
                // only validate for version no exist, means > 0
                checkVersion( block, value, key);
                block.setNode( value.getNode());
                long dif = value.getData().length - block.getDataLen() ;
                //block.setData( value.getData());
                updateReference( block, value.getData() );
                if (useMaxCache) currentMemory.getAndAdd( dif );
                block.setDataLen( value.getData().length );
                //if ( block.getData() == null ) {
                checkFileSize( block.getDataOffset() , value.getData().length );
                if ( value.getData().length > block.getBlockSize() ) {
                    int size = findBlockSize( value.getData().length );
                    if ( size == 0 ) logger.warn("size = 0  key "+key.toString() );
                    // record overflow
                    overflow.addAndGet( block.getBlockSize());
                    byte[] keyBytes = toKeyBytes(key);
                    lock.lock();
                    try {
                        block.setDataOffset( channel.getDataOffset());
                        block.setBlockSize( size);
                        block.setDataLen( value.getData().length);
                        channel.setDataOffset( channel.getDataOffset()+ size) ;
                    } finally {
                        lock.unlock();
                    }
                    //logger.info("offset "+ channel.getDataOffset()+" block "+block.toString());
                    keyOffset2Len = convertOffset4Len( channel.getKeyOffset(), keyBytes.length);
                    try {
                        // don't call writeNewBlock
                        channel.writeExistBlock(block);
                    } catch (IOException ex) {
                        logger.error( ex.getMessage(), ex);
                        throw new StoreException(ex.getMessage());
                    } finally {
                        if ( isNoCache() ) block.setValue( null);
                    }
                }
                else {
                    //logger.info("offset "+ channel.getDataOffset()+" block "+block.toString());
                    try {
                        channel.writeExistBlock(block);
                    } catch (IOException ex) {
                        logger.error( ex.getMessage(), ex);
                        throw new StoreException(ex.getMessage());
                    } finally {
                         if ( isNoCache() ) block.setValue( null);
                    }
                }
            }
        }

    }

    // setup default
    public final static int MAX_DRAIN_SIZE = 1000;
    private int maxDrain = Math.max(19, Runtime.getRuntime().availableProcessors() );
    public void setMaxDrain(int maxDrain){
        if ( maxDrain > 0 && maxDrain <  MAX_DRAIN_SIZE  ) {
            this.maxDrain = maxDrain;
        }
        else logger.warn("invalid maxDrain "+maxDrain);
    }

    private boolean needToDrain(CacheBlock block) {

        if ( doubleLinkedList.isFull() ) {
            if ( actionQueue.size() < 1 ) {
                try {
                    actionQueue.offer(getRecord(block) ,200L, TimeUnit.MILLISECONDS );
                 } catch (InterruptedException e) {
                     logger.error("fail to offer "+block.toString()+" "+e.getMessage());
                 }
            }
            else logger.info("skip "+ getRecord(block));
            return true;
        }
        else return false;
    }

    private String getRecord(CacheBlock block) {
        return "# "+block.getRecordNo() ;
    }

    private void createReference(CacheBlock<byte[]> block, byte[] data) {
        if ( useMaxCache ) {
            StrongReferenceImpl<byte[]> ref = new StrongReferenceImpl<byte[]>( data);
            block.setValue( ref);
        }
        else {
            Reference<byte[]> ref = new SoftReferenceImpl<byte[]>(data);
            block.setValue( ref);
        }

    }

    private void updateReference(CacheBlock<byte[]> block, byte[] data) {
        if ( block.getValue() == null ) createReference( block, data);
        else {
            if (useMaxCache ) {
                block.setData( data);
                //needToMove( block);
            } else {
                Reference<byte[]> ref = new SoftReferenceImpl<byte[]>(data);
                block.setValue( ref);
            }
        }
    }

    public long getDeleted(){ return deleted; }

    public AtomicLong getCurrentMemory() {
        return currentMemory;
    }

    private void needPackDate() {
        if ( overflow.get() > trigger) {
            logger.info("Need to pack data curIndex " +curIndex );
            pack(0);
        }
    }

    /**
     *
     * @param key
     * @return   true for successful , false for fail
     */
    public boolean remove(Key key) {
        CacheBlock block = map.get( key);
        if ( block == null ) return false;
        else {
            synchronized (block) {
                try {
                    block.setDelete( true);
                    deleted ++;
                    if ( delayWrite && block.getRecordNo() == INIT_RECORDNO )
                        logger.info("skip removeBlock RecordNo == 0 key "+key.toString());
                    else
                        getChannel( block ).removeBlock(block);
                    // record over flow size
                    overflow.addAndGet(block.getBlockSize());
                    if ( block.getData() != null)
                        if (useMaxCache) currentMemory.getAndAdd( -( (byte[]) block.getData()).length );
                    map.remove( key);
                    // need to delink
                    return true;
                } catch (Exception ex) {
                    logger.error(ex.getMessage() , ex);
                    return false;
                }
            }
        }
    }

    /**
     * close all channel file for this cachestore
     */
    public void close() {
        if ( serverState != State.Active ) logger.warn("Wait for "+ serverState + " to be completed");
        packLock.lock();
        try {
            serverState = State.Shutdown ;
            //disable the wait
//            try {
//                // sleep four seconds
//                Thread.sleep(1000L);
//            } catch (InterruptedException e) {
//                // swallow exception
//            }
            for ( int i = 0 ; i < list.size() ; i++)
                list.get(i).close(delayWriteQueue);
            // shutdown back ground task
            if ( packTask != null ) packTask.cancel();
            if ( purgeTask != null ) purgeTask.cancel();
            if ( backupTask != null ) backupTask.cancel();
            //if ()
        } finally {
            packLock.unlock();
        }
    }

    public void backup(String path) {
        backup(path, 0);
    }

    public void backup(String path, int threshold) {
        packLock.lock();
        try {
            checkPath(path);
            Date date = new Date();
            String filename = path+"/"+getNamePrefix()+getCurIndex()+"."+date.getMonth()+"."+date.getDate()
                    +"."+date.getHours()+"."+date.getMinutes();
            backupTask = new BackupThread(this, curIndex, filename);
            backupTask.run();
            //new BackupThread(this, curIndex, filename).start(threshold);
        } finally {
            packLock.unlock();
        }
    }

    /**
     * pack will sprung a back groud thread to pack list.get(curIndex), and move all data to a file ( 0 -> 1 and 1 -> 0)
     * each one would ast as backup of the other one
     */
    public void pack(int threshold) {
        // user trigger the pack data, force is true
        logger.info("Trigger by user ...");
        //new PackThread( this, true, threshold ).run();
        packTask = new PackThread( this, true, threshold );
        packTask.run();
    }

    public String getNamePrefix() {
        return namePrefix;
    }

    public State getServerState() {
        return serverState;
    }

    public boolean isShutDown() {
        return serverState == State.Shutdown ;
    }

    public void setServerState(State serverState) {
        this.serverState = serverState;
    }


    public AtomicLong getCacheHit() {
        return cacheHit;
    }

    public AtomicLong getCacheMis() {
        return cacheMis;
    }

    /**
     * to determine which index to use for packing data, use mode 2 logic
     * @param index - current index
     * @return
     */
    public int createChannel(int index, String filename) {
        try {
            //take module 2, to use it alternative for pack data ( 0 - > 1)
            int i = 0;
            if ( index % 2 == 0 )
                i = index + 1 ;
            else
                i = index - 1;
            logger.info("Add channel "+ i );
            if ( list.size() > i  ) {
                list.get(i).close(delayWriteQueue);
                list.set(i, open( getPath(path) + filename + i, map, true, i,  delayWrite, mode));
            }
            else
                list.add(i, open( getPath(path)+ filename + i, map, true, i, delayWrite, mode));

            return i ;
        } catch (Exception ex) {
            throw new StoreException(ex.getMessage(), ex);
        }
    }

    /**
     * removeChannels all channels for same prefix
     *
     *
     * @param index
     */
    public void removeChannels(int index) {
        logger.info("removeChannels "+index);
        list.get(index).forceClose();
        list.set(index, open( getPath(path) + getNamePrefix() + index, map, true, index, delayWrite, mode));
        list.get(index).forceClose();
    }

    public ReentrantLock getPackLock() {
        return packLock;
    }

    public AtomicLong getOverflow() {
        return overflow;
    }

    public void setDeleted(long deleted) {
        this.deleted = deleted;
    }

    public long getTrigger() {
        return trigger;
    }

    public void setTrigger(long trigger) {
        this.trigger = trigger * 1024 ;
    }

    public AccessQueue<Reference> getDoubleLinkedList() {
        return doubleLinkedList;
    }

    /**
     * @param period
     */
    public void startTask(long period, Purge purge) {
        if ( period > 0 ) {
            logger.info("Start purge thread");
            purgeTask = new PurgeThread(purge, this);
            new Timer("PurgeCache").schedule( purgeTask, period,  period);
        }
    }

    public String validateLink() {
        if ( doubleLinkedList != null )
            return ((DoubleLinkedList) doubleLinkedList).validLink();
        else return "not available";
    }

    public void writeBack(CacheBlock block, Key key) {
        if ( block == null ) logger.error("key is not in map "+ key.toString()+" "+block.toString() );
        else {
            ChannelStore channel = list.get(block.getIndex());
            byte[] keyBytes = toKeyBytes( key);
            //reset Dirty bit before write
            block.setDirty( false);
            // check for add mode, two conditions, new record or overflow record
            if ( block.getDataOffset() == 0) {
                // lock for multiPuts dataOffset, total record, keyOffset
                long keyOffset2Len = 0;
                // indicate existing block, default to true
                boolean old = true;
                lock.lock();
                try {
                    keyOffset2Len = convertOffset4Len( channel.getKeyOffset(), keyBytes.length);
                    block.setDataOffset(channel.getDataOffset() );
                    channel.setDataOffset(channel.getDataOffset() + block.getBlockSize());
                    // multiPuts total record for new record only
                    if( block.getRecordNo() == INIT_RECORDNO ) {
                        // new block, set old flag to true
                        old = false;
                        block.setRecordNo( channel.getTotalRecord() );
                        channel.setTotalRecord(channel.getTotalRecord() + 1);
                        channel.setKeyOffset(channel.getKeyOffset() + keyBytes.length);
                    }
                } finally {
                    lock.unlock();
                }
                try {
                    if ( old )
                        channel.writeExistBlock(block);
                    else
                        channel.writeNewBlock(block, keyOffset2Len, keyBytes);
                } catch( IOException ex) {
                    //swallow exception
                    logger.error( ex.getMessage()+" "+block.toString()+" "+key.getKey().toString(), ex);
                }
            }
            else { //multiPuts mode
                try {
                     channel.writeExistBlock(block);
                 } catch (IOException ex) {
                     logger.error( ex.getMessage()+" "+block.toString()+key.getKey().toString(), ex);
                     //throw new StoreException(ex.getMessage());
                 }
            }
        }

    }

   public class WriteBackThread extends Thread {
       private volatile long dirtyNo = 0;
       private volatile long emptyNo = 0;
       private volatile long count =0;

       public WriteBackThread(int no) {
            setPriority(Thread.MAX_PRIORITY);
            setName("WriteBackThread-"+no);
            setDaemon(true);
       }
       public String getStat() {
            return "dirty "+dirtyNo+" empty "+emptyNo+" count "+count ;
       }

       public long getDirtyNo() {
           return dirtyNo;
       }

       public long getEmptyNo() {
           return emptyNo;
       }

       public long getCount() {
           return count;
       }

       @Override
        public void run() {
            while (true) {
                try {
                    while (true) {
                        DelayBlock delayBlock = delayWriteQueue.take();
                        // skip if it had been GCed or not dirty
                        if( ! delayBlock.getBlock().isDirty() || delayBlock.getBlock().getData() == null ){
                            if ( delayBlock.getBlock().getData() == null) {
                                emptyNo ++ ;
                                logger.error("rec# " + delayBlock.getBlock().getRecordNo() + " key " + delayBlock.getKey().toString());
                            }
                            else {
                                dirtyNo ++;
                            }
                            continue;
                        }
                        // skip the block , if it is not dirty
                        synchronized ( delayBlock.getBlock()) {
                            //ChannelStore channel = getChannel( delayBlock.getBlock());
                            writeBack(delayBlock.getBlock(), delayBlock.getKey());
                            if ( isNoCache() ) delayBlock.getBlock().setValue( null);
                            count++;
                        }
                    }
                } catch (Throwable e) {
                    logger.warn("dblk "+e.getMessage(),e);
                }
            }
        }
    }

    public String getCacheMemoryStat() {
        return "Max "+maxCacheMemory+" current "+currentMemory.get();
    }
    private boolean isOverCacheMemory() {
        if ( currentMemory.get() > maxCacheMemory) return true;
        else return false;
    }

    public class DrainQueueThread extends Thread {
        volatile long drain = 0;
        volatile long skip = 0;
        volatile long total = 0;


        public DrainQueueThread() {
            setPriority(Thread.MAX_PRIORITY);
            setName("DrainQueue");
            setDaemon(true);

        }

        public String getStat() {
            return "drain  "+ drain +" skip "+ skip +" total "+ total ;
        }

        public void run() {
            if (doubleLinkedList == null ) {
                logger.warn("reference queue is null, stop thread");
                return;
            }
            // take max(1000 or 1% of capacity)
            int stop = Math.max(doubleLinkedList.getCapacity() - maxDrain,  MAX_DRAIN_SIZE ) ;
            while ( true ) {
                try {
                    String action = actionQueue.take();
                    total++;
                    if ( doubleLinkedList.isFull() ) {
                        drain ++;
                        int size = doubleLinkedList.size();
                        int times = 0 ;
                        logger.info(action+" begin "+size+" stop "+stop);
                        for ( ; size >= stop ; size --  ) {
                            Reference ref = doubleLinkedList.evictNextHead();
                            if ( ref == null ){
                                logger.info("stop at times "+times);
                                break;
                            }
                            else {
                                synchronized (ref) {
                                    ref.setData(null);
                                    //ref.setPrev(null);
                                    //ref.setNext(null);
                                }
                                times++;
                                currentMemory.getAndAdd( -((CacheBlock)ref).getDataLen() );
                            }
                        }
                    }
                    else skip++;
                } catch (Throwable th) {
                    logger.error( th.getMessage(), th);
                }
            }
        }
    }



}

