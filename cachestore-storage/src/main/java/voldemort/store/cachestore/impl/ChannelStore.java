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

/**
 * Created by IntelliJ IDEA.
 * User: mhsieh
 * Date: 1/25/11
 * Time: 1:58 PM
 * To change this template use File | Settings | File Templates.
 */
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import voldemort.store.cachestore.BlockOps;
import voldemort.store.cachestore.CacheBlock;
import voldemort.store.cachestore.Key;
import voldemort.store.cachestore.StoreException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import static voldemort.store.cachestore.BlockUtil.*;

public class ChannelStore implements BlockOps<byte[]> {

    private static Log logger = LogFactory.getLog(ChannelStore.class);
    // when data reside
    private FileChannel dataChannel;
    // key for map reside, it is write once
    private FileChannel keyChannel;
    // fixed len index block for dataChannel and keyChannel
    private FileChannel indexChannel;
    // delete log file
    //private FileChannel logChannel;
    // signature of data and index file
    public static final int MAGIC = 0xBABECAFE;
    // beginning of index channel
    public final static int OFFSET = 4;
    // total number of record
    private volatile int totalRecord = 0 ;
    // current offset of dataf
    private volatile long dataOffset = 0;
    // key channel offset
    private volatile long keyOffset = 0;
    // logOffset
    //private volatile long logOffset = 0;
    //private final ReentrantLock lock = new ReentrantLock();
    // number of deleted record
    private volatile int deleted;
    // plug in interface to determine blockSize
    //private BlockSize blockSize;
    // map hold all key in memory and cacheBlock. Data may be, data is cacheable, but key is not
    private Map<Key, CacheBlock> map;
    // filename
    private String filename;
    // delayWrite
    private boolean delayWrite;
    // channel index
    private int index;
    // number of error
    private int error ;


    private ChannelStore(String filename, Map map, boolean reset, int index, boolean delayWrite) {
        this(filename, map, reset, index, delayWrite, 0);
    }

    private ChannelStore(String filename, Map map, boolean reset, int index, boolean delayWrite, int mode) {
        String type = "rw" ;
        if ( mode % 3 == 1) type = "rwd";
        else if (mode % 3 == 2) type = "rws";
        try {
            this.index = index;
            this.delayWrite = delayWrite;
            this.map = map;
            this.filename = filename ;
            // add rwd mode to avoid file system cache
            indexChannel = new RandomAccessFile(filename+".ndx", type).getChannel();
            checkSignature( indexChannel);
            keyChannel   = new RandomAccessFile(filename+".key", type).getChannel();
            checkSignature( keyChannel);
            dataChannel  = new RandomAccessFile(filename+".data", type).getChannel();
            checkSignature( dataChannel);
            //logChannel  = new RandomAccessFile(filename+".log", type).getChannel();
            //checkSignature( logChannel);
            init( reset);
        } catch (IOException ex) {
            throw new StoreException(ex.getMessage(), ex);
        }
    }


    public static ChannelStore open(String filename, Map map, boolean reset, int index, boolean delay) {
        return new ChannelStore(filename, map, reset, index, delay);
    }

    public static ChannelStore open(String filename, Map map, boolean reset, int index, boolean delay, int mode) {
        return new ChannelStore(filename, map, reset, index, delay, mode);
    }

    public String getFilename() {
        return filename;
    }


    public static boolean isChannelExist(String filename) {
        File file = new File( filename);
        if ( ! file.isFile() ) return false;
        if ( file.length() >=  OFFSET + RECORD_SIZE )
            return true;
        else
            return false;
    }

    private boolean checkSignature(FileChannel channel) throws IOException {
        ByteBuffer intBytes = ByteBuffer.allocate(OFFSET);
        if ( channel.size() == 0) {
           intBytes.putInt(MAGIC);
           intBytes.flip();
           channel.write(intBytes);
        }
        else {
           channel.read(intBytes);
           intBytes.rewind();
           int s = intBytes.getInt();
           if ( s != MAGIC )
                throw new StoreException("Header mismatch expect "+ Integer.toHexString(MAGIC)+" read "+
                        Integer.toHexString(s) );
        }
        return true;
    }

    private void init( boolean reset) throws IOException {
        if ( reset ) {
            indexChannel.truncate( OFFSET);
            dataChannel.truncate( OFFSET);
            keyChannel.truncate( OFFSET);
            totalRecord = 0 ;
        }
        else {
            long length = indexChannel.size() - OFFSET ;
            totalRecord = (int) ( length / RECORD_SIZE) ;
            ByteBuffer buf = ByteBuffer.allocate(RECORD_SIZE);
            logger.info("Building key map and read index file for "+filename+" total record " + totalRecord  );
            long per = 0; int j =0 ;
            if ( totalRecord >= 1000000) per = totalRecord / 10 ;

            for ( int i = 0 ; i < totalRecord ; i ++) {
                indexChannel.read(buf);
                assert ( buf.capacity() == RECORD_SIZE);
                buf.rewind();
                byte status = buf.get();
                if ( isDeleted(status) ) this.deleted ++ ;
                else {
                    long key = buf.getLong();
                    byte[] keys;
                    try {
                        keys = readChannel( key, keyChannel);
                        long data = buf.getLong();
                        long block2version = buf.getLong();
                        CacheBlock block = new CacheBlock(i, data, block2version, status );
                        map.put( toKey( keys) , block);
                    } catch (Exception ex) {
                        logger.warn("Not able to read record no "+i+" , skip reason "+ex.getMessage());
                        buf.clear();
                        error ++;
                        continue;
                    }
                }
                buf.clear();
               if( per > 0 &&  (i +1) % per == 0 ) {
                    logger.info( (++j * 10) + "% complete");
                }
            }
        }
        dataOffset = dataChannel.size();
        keyOffset = keyChannel.size();
        //logOffset = logChannel.size();
        logger.info("Total record " + totalRecord + " deleted "+deleted+" error "+error + " active "+ (totalRecord - deleted -error) );
    }

    private boolean isDeleted(byte b) {
        if ( (b & DELETED) == DELETED ) return true;
        else return false;
    }

    public boolean isDelayWrite() {
        return delayWrite;
    }

    public int getIndex() {
        return index;
    }

    public void forceFlush() {
        forceFlush( indexChannel);
        forceFlush( dataChannel);
        forceFlush( keyChannel);
        //forceFlush( logChannel);
    }

    private void forceFlush(FileChannel channel) {
        try {
            channel.force(false);
        } catch (IOException e) {
            //swallow exception
            logger.error( e.getMessage(), e);
        }
    }

    /**
     *
     * @param offset2len
     * @param channel
     * @return
     * @throws IOException
     */
    public byte[] readChannel(long offset2len, FileChannel channel)  throws IOException {
        long offset = getOffset( offset2len);
        int len = getLen( offset2len);
        ByteBuffer data = ByteBuffer.allocate( len);
        channel.read( data, offset);
        return data.array();
    }


    public void writeNewBlock(CacheBlock<byte[]> block, long keyOffset2Len, byte[] key) throws IOException {
        // write in sequence of data, key and index channel, which doen not need call flip()
        ByteBuffer dataBuf = ByteBuffer.wrap( block.getData() );
        dataChannel.write( dataBuf, block.getDataOffset() );
        // position to end of block
        if (block.getBlockSize() > block.getDataLen()  ) {
            ByteBuffer fillBuf = ByteBuffer.wrap( new byte[] {0} );
            dataChannel.write( fillBuf, block.getDataOffset()+ block.getBlockSize()-1 );
        }
        ByteBuffer keyBuf = ByteBuffer.wrap( key );
        keyChannel.write( keyBuf, getOffset(keyOffset2Len));
        writeIndexBlock(block, keyOffset2Len, indexChannel );
    }

    public void writeIndexBlock(CacheBlock<byte[]> block, long keyOffset2Len, FileChannel channel ) throws IOException {
        long pos = OFFSET + (long) block.getRecordNo() * RECORD_SIZE;
        checkFileSize(pos, RECORD_SIZE );
        ByteBuffer buf = ByteBuffer.allocate( RECORD_SIZE);
        buf.put(block.getStatus() );
        buf.putLong(keyOffset2Len );
        buf.putLong(block.getDataOffset2Len() );
        buf.putLong(block.getBlock2Version() );
        buf.putShort(block.getNode() );
        buf.flip();
        channel.write( buf, pos);
    }


    public void writeExistBlock(CacheBlock<byte[]> block) throws IOException {
        ByteBuffer dataBuf = ByteBuffer.wrap( block.getData() );
        // don't use flip() before it write when use wrap
        //dataBuf.flip();
        dataChannel.write( dataBuf, block.getDataOffset() );
        long pos = OFFSET + (long) block.getRecordNo() * RECORD_SIZE;
        int off = 9;
        ByteBuffer buf = ByteBuffer.allocate( RECORD_SIZE - off);
        buf.putLong(block.getDataOffset2Len() );
        buf.putLong(block.getBlock2Version() );
        buf.putShort(block.getNode() );
        // must flip() before it write
        buf.flip();
        indexChannel.write( buf, pos + off);
    }

    public void removeBlock(CacheBlock<byte[]> block) throws IOException{
        long pos = OFFSET + (long) block.getRecordNo() * RECORD_SIZE;
        ByteBuffer buf = ByteBuffer.allocate( 1);
        // set delete by calling program
        byte status = block.getStatus() ;
        buf.put( status );
        buf.flip();
        indexChannel.write( buf, pos);
        //remove this, use logfile for bootstrap with pre load function
        //writeIndexBlock(block, logOffset, logChannel);
        //logOffset += RECORD_SIZE ;
    }

    private void close(FileChannel channel) {
        try {
            if ( channel != null )
                channel.close();
        } catch (IOException ex) {
            logger.error( ex.getMessage(), ex);
            // swallow exception
        }
    }

//    public void writeBack(CacheBlock block, Key key) {
//        if ( block == null ) logger.error("key is not in map "+ key.toString());
//        else {
//            // if it is dirty, channel no did not match skip
//            //if ( !block.isDirty() || block.getIndex() != index ) {
//            //    //logger.info("key "+key.toString()+" "+block.isDirty()+" "+block.getIndex()+" index "+index);
//            //    return;
//            //}
//            byte[] keyBytes = toKeyBytes( key);
//            //reset Dirty bit before write
//            block.setDirty( false);
//            // check for add mode, two conditions, new record or overflow record
//            if ( block.getDataOffset() == 0) {
//                // lock for multiPuts dataOffset, total record, keyOffset
//                long keyOffset2Len = 0;
//                // indicate existing block, default to true
//                boolean old = true;
//                lock.lock();
//                try {
//                    keyOffset2Len = convertOffset4Len( getKeyOffset(), keyBytes.length);
//                    block.setDataOffset(getDataOffset() );
//                    setDataOffset( getDataOffset() + block.getBlockSize());
//                    //keyOffset2Len = convertOffset4Len( getKeyOffset(), keyBytes.length);
//                    // multiPuts total record for new record only
//                    if( block.getRecordNo() == INIT_RECORDNO ) {
//                        // new block, set old flag to true
//                        old = false;
//                        block.setRecordNo( getTotalRecord() );
//                        setTotalRecord(getTotalRecord()+1);
//                        setKeyOffset( getKeyOffset() + keyBytes.length );
//                    }
//
//                } finally {
//                    lock.unlock();
//                }
//                try {
//                    if ( old )
//                        writeExistBlock( block);
//                    else
//                        writeNewBlock(block, keyOffset2Len, keyBytes);
//                } catch( IOException ex) {
//                    //swallow exception, nothing can be done for demoan thread
//                    logger.error( ex.getMessage()+" "+block.toString()+" "+key.getKey().toString(), ex);
//                }
//            }
//            else { //multiPuts mode
//                try {
//                     writeExistBlock(block);
//                 } catch (IOException ex) {
//                     logger.error( ex.getMessage()+" "+block.toString()+key.getKey().toString(), ex);
//                     //throw new StoreException(ex.getMessage());
//                 }
//            }
//        }
//
//    }

//    private void flush() {
//        logger.info("Flush all data before closing channel "+index);
//        Iterator<Key> its = map.keySet().iterator();
//        while ( its.hasNext() ) {
//            Key key = its.next();
//            CacheBlock<byte[]> block = map.get( key);
//            writeBack(block, key);
//        }
//    }


    public void forceClose() {
        close( dataChannel);
        close( keyChannel);
        close( indexChannel);
        //close( logChannel);

    }

    public void close(BlockingQueue delayQueue) {
        if ( delayWrite ) {
            if ( delayQueue.size() == 0 ) {
                logger.info("delayQueue is empty");
                return;
            }
            else {

                int size = delayQueue.size();
                for ( int i = 0 ; i < size ; i++ ) {
                    try {
                        Thread.sleep( 200L);
                        logger.info("delayQueue size "+ delayQueue.size() +" i ="+i);
                        // return when there is no item in queue
                        if ( delayQueue.size() == 0 ) {
                            logger.info("delayQueue is empty");
                            return;
                        }
                    } catch (Exception ex) {
                      // swallow
                    }
                }
                logger.error("after "+ size /5 +" seconds, buffer size "+ delayQueue.size());
            }
        }
        else close();

    }

    public void close() {
        logger.info("Channel store close index "+ index);
        forceClose();
    }

    /**
     * get keyOffset and len from index channel
     * @param record #
     * @return key Object
     */
    public Key readKey(int record) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(RECORD_SIZE);
        indexChannel.read( buf, (long) record * RECORD_SIZE + OFFSET);
        buf.rewind();
        byte status = buf.get();
        if ( isDeleted(status) ) return null ;
        else {
            long key = buf.getLong();
            byte[] keys = readChannel( key, keyChannel);
            return toKey( keys);
        }
    }

    public FileChannel getDataChannel() {
        return dataChannel;
    }

    public FileChannel getKeyChannel() {
        return keyChannel;
    }

    public FileChannel getIndexChannel() {
        return indexChannel;
    }

//    public FileChannel getLogChannel() {
//        return logChannel;
//    }

    public int getTotalRecord() {
        return totalRecord;
    }

    public long getDataOffset() {
        return dataOffset;
    }

    public long getKeyOffset() {
        return keyOffset;
    }

//    public long getLogOffset() {
//        return logOffset;
//    }

    public int getDeleted() {
        return deleted;
    }

    public void setTotalRecord(int totalRecord) {
        this.totalRecord = totalRecord;
    }

    public void setDataOffset(long dataOffset) {
        this.dataOffset = dataOffset;
    }

    public void setKeyOffset(long keyOffset) {
        this.keyOffset = keyOffset;
    }

//    public void setLogOffset(long logOffset) {
//        this.logOffset = logOffset;
//    }


    public void truncate() throws IOException {
        indexChannel.truncate( OFFSET);
        dataChannel.truncate( OFFSET);
        keyChannel.truncate( OFFSET);
    }
}

