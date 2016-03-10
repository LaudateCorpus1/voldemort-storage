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
import voldemort.store.cachestore.voldeimpl.BlockValue;
import voldemort.store.cachestore.voldeimpl.KeyValue;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.ReentrantLock;

import static voldemort.store.cachestore.BlockUtil.*;
import static voldemort.store.cachestore.impl.ChannelStore.*;

/**
 * Created by IntelliJ IDEA.
 * User: mhsieh
 * Date: 4/12/12
 * Time: 11:01 AM
 * To change this template use File | Settings | File Templates.
 */
public class LogChannel {
    private static Log logger = LogFactory.getLog(LogChannel.class);
    // when data reside
    private FileChannel dataChannel;
    // key for map reside, it is write once
    private FileChannel keyChannel;
    // fixed len index block for dataChannel and keyChannel
    private FileChannel indexChannel;
    // total number of record
    private volatile int totalRecord = 0 ;
    // current offset of dataf
    private volatile long dataOffset = 0;
    // key channel offset
    private volatile long keyOffset = 0;
    // map hold all key in memory and cacheBlock. Data may be, data is cacheable, but key is not
    private final ReentrantLock lock = new ReentrantLock();
    // number of deleted record
    private String filename;
    private int index ;
    private String path;
    private volatile boolean lastRecord;

    public LogChannel(String filename, int index, String path) {
        this(filename, index, path, 0);
    }

    public LogChannel(String filename, int index, String path, int mode) {
        this.path= path;
        checkPath(path);
        String type = "rw" ;
        if ( mode % 3 == 1) type = "rwd";
        else if (mode % 3 == 2) type = "rws";
        try {
            this.index = index;
            this.filename = filename ;
            // add rwd mode to avoid file system cache
            indexChannel = new RandomAccessFile(path+"/"+filename+".ndx", type).getChannel();
            checkSignature( indexChannel);
            keyChannel   = new RandomAccessFile(path+"/"+filename+".key", type).getChannel();
            checkSignature( keyChannel);
            dataChannel  = new RandomAccessFile(path+"/"+filename+".data", type).getChannel();
            checkSignature( dataChannel);
            init();
        } catch (IOException ex) {
            throw new StoreException(ex.getMessage(), ex);
        }
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

    private void init() throws IOException {
        long length = indexChannel.size() - OFFSET ;
        totalRecord = (int) ( length / RECORD_SIZE) ;
        dataOffset = dataChannel.size();
        keyOffset = keyChannel.size();

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
        channel.write(buf, pos);
    }

    private void writeIndexBlock(long keyOffset2Len, int record, long dataOffset2Len, short nodeId, long block2Version, byte status) throws IOException {
       long pos = OFFSET + (long) record * RECORD_SIZE;
        checkFileSize(pos, RECORD_SIZE );
        ByteBuffer buf = ByteBuffer.allocate( RECORD_SIZE);
        buf.put( status );
        buf.putLong(keyOffset2Len );
        buf.putLong(dataOffset2Len );
        buf.putLong(block2Version );
        buf.putShort(nodeId );
        buf.flip();
        getIndexChannel().write(buf, pos);
    }

    public byte[] readChannel(long offset2len, FileChannel channel)  throws IOException {
        long offset = getOffset( offset2len);
        int len = getLen( offset2len);
        ByteBuffer data = ByteBuffer.allocate( len);
        channel.read( data, offset);
        return data.array();
    }

    public void close() {
        logger.info("Channel store close index " + index);
        close( dataChannel);
        close( keyChannel);
        close( indexChannel);
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

    public FileChannel getDataChannel() {
        return dataChannel;
    }

    private boolean isLastRecord(int recordNo) {
        if ( recordNo == totalRecord && lastRecord )
            return true;
        else
            return false;
    }

   public KeyValue readRecord(int recordNo) {
        if ( isEOF(recordNo)) throw new RuntimeException("record out of range "+getTotalRecord()+" expected "+recordNo );
        ByteBuffer buf = ByteBuffer.allocate(RECORD_SIZE);
        long pos = ChannelStore.OFFSET + (long) recordNo * RECORD_SIZE;
        try {
            if ( isLastRecord( recordNo)) {
                logger.info("skip due to "+totalRecord+" read "+recordNo );
                return null;
            }
            getIndexChannel().read(buf, pos);
            assert ( buf.capacity() == RECORD_SIZE);
            buf.rewind();
            byte status = buf.get();
            long keyOffset2Len = buf.getLong();
            byte[] keys = readChannel(keyOffset2Len, getKeyChannel());
            Key k = toKey( keys);
            long dataOffset2Len = buf.getLong();
            byte[] datas = readChannel(dataOffset2Len, getDataChannel());
            long block2version = buf.getLong();
            Value<byte[]> value = null;
            //if delete return value=null, not delete read value
            if ( ! isDeleted(status))
                value = new BlockValue<byte[]>( datas, BlockUtil.getVersionNo(block2version), (short) 0);
            return new KeyValue( k, value);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public int writeRecord(Key key, Value<byte[]> value) {
        int len = value.getData().length;
        byte[] keyBytes = toKeyBytes(key);
        lock.lock();
        long offset, kOffset;
        int record= 0;
        try {
            lastRecord = true;
            offset = dataOffset;
            record = totalRecord;
            kOffset = keyOffset;
            dataOffset += value.getData().length;
            keyOffset += keyBytes.length;
            totalRecord ++;
        } finally {
            lock.unlock();
        }
        try {
            long keyOffset2Len = convertOffset4Len( kOffset, keyBytes.length);
            long dataOffset2Len = convertOffset4Len(offset, len);
            long block2Version =  convertVersion4Size( value.getVersion(), len );
            getDataChannel().write( ByteBuffer.wrap( value.getData()), offset);
            getKeyChannel().write( ByteBuffer.wrap(keyBytes), kOffset );
            writeIndexBlock( keyOffset2Len, record, dataOffset2Len, value.getNode(), block2Version, (byte) 0);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            lastRecord = false;
            return record;
        }
    }

    public int writeDelete(Key key) {
        byte[] keyBytes = toKeyBytes(key);
        lock.lock();
        long offset, kOffset;
        int record= 0;
        try {
            offset = dataOffset;
            record = totalRecord;
            kOffset = keyOffset;
            keyOffset += keyBytes.length;
            totalRecord ++;
        } finally {
            lock.unlock();
        }
        try {
            long keyOffset2Len = convertOffset4Len( kOffset, keyBytes.length);
            long dataOffset2Len = convertOffset4Len(offset, 0);
            long block2Version =  convertVersion4Size( 0, 0 );
            getKeyChannel().write( ByteBuffer.wrap(keyBytes), kOffset );
            writeIndexBlock( keyOffset2Len, record, dataOffset2Len, (short) 0 , block2Version, DELETED);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            return record;
        }
    }

    public boolean isEOF(int record) {
        try {
            if ( indexChannel.size() > OFFSET + (long) record * RECORD_SIZE ) return false;
            else return true;
        } catch (IOException io) {
            //logger.info("EOF "+io.getMessage() );
            return true;
        }
    }

    public int getComputeTotal() {
        try {
            return (int) (indexChannel.size() - OFFSET) / RECORD_SIZE;
        } catch (IOException io) {
            return -1 ;
        }
    }

    @Override
    public String toString() {
        return path+"/"+filename+"."+index+" cur # "+totalRecord ;
    }

    public String getPath() {
        return path;
    }

    public int getIndex() {
        return index;
    }

    public String getFilename() {
        return filename;
    }

    public FileChannel getKeyChannel() {
        return keyChannel;
    }

    public FileChannel getIndexChannel() {
        return indexChannel;
    }

    public int getTotalRecord() {
        return totalRecord;
    }

    public void setTotalRecord(int totalRecord) {
        this.totalRecord = totalRecord;
    }

    public long getDataOffset() {
        return dataOffset;
    }

    public void setDataOffset(long dataOffset) {
        this.dataOffset = dataOffset;
    }

    public long getKeyOffset() {
        return keyOffset;
    }

    public void setKeyOffset(long keyOffset) {
        this.keyOffset = keyOffset;
    }
}
