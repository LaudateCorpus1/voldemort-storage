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


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import voldemort.store.cachestore.CacheBlock;
import voldemort.store.cachestore.Key;
import voldemort.store.cachestore.StoreException;
import voldemort.store.cachestore.impl.ChannelStore;
import voldemort.utils.Pair;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static voldemort.store.cachestore.BlockUtil.*;
import static voldemort.store.cachestore.impl.CacheValue.createValue;

/**
 * Created with IntelliJ IDEA.
 * User: mhsieh
 * Date: 8/16/13
 * Time: 10:53 AM
 * To change this template use File | Settings | File Templates.
 */
public class StoreIterator {
    private static Log logger = LogFactory.getLog(StoreIterator.class);
    public final static int MAGIC = 0xBABECAFE;
    // beginning of index channel
    public final static int OFFSET = 4;

    protected String filename;
    //private CacheStore store;
    protected FileChannel dataChannel;
    // key for map reside, it is write once
    protected FileChannel keyChannel;
    // fixed len index block for dataChannel and keyChannel
    protected FileChannel indexChannel;
    protected int totalRecord;
    //recordCount interface, implemented by
    protected int curRecord ;
    protected boolean deleteFlag;
    protected long version;

    public StoreIterator(String filename, boolean deleteFlag) {
        this.filename = filename;
        this.deleteFlag = deleteFlag;
        init();
    }

    public StoreIterator(String filename) {
        this(filename, false);
    }

    protected void init() {
        try {
            String index = findChannel( filename);
            if (deleteFlag ) {
                String type = "rw";
                indexChannel = new RandomAccessFile(filename + index + ".ndx", type).getChannel();
                checkSignature(indexChannel);
                keyChannel = new RandomAccessFile(filename + index + ".key", type).getChannel();
                checkSignature(keyChannel);
                dataChannel = new RandomAccessFile(filename + index + ".data", type).getChannel();
                checkSignature(dataChannel);
            }
            else {
                indexChannel = new FileInputStream( new File(filename+index+".ndx")).getChannel();
                keyChannel = new FileInputStream( new File(filename+index+".key")).getChannel();
                dataChannel = new FileInputStream( new File(filename+index+".data")).getChannel();
                checkSignature( indexChannel);
            }
            long length = indexChannel.size() - OFFSET ;
            totalRecord = (int) ( length / RECORD_SIZE) ;
            curRecord = 0 ;

        } catch (Exception e) {
            throw new RuntimeException( e.getMessage(), e);
        }
    }

    public static String findChannel(String filename) {
        if (ChannelStore.isChannelExist(filename + ".ndx")) {
            return "";
        }
        if ( ChannelStore.isChannelExist( filename + 0 + ".ndx"))
            return "0";
        else if ( ChannelStore.isChannelExist( filename + 1 + ".ndx"))
            return "1";
        else throw new RuntimeException("can not find file channel");
    }

    public boolean hasNext() {
        if ( curRecord < totalRecord ) return true;
        else return false;
    }

    protected ByteBuffer buf = ByteBuffer.allocate(RECORD_SIZE);
    /**
     *
     * @return null if record is deleted, tuple of bytes is
     * @throws IOException for file IO error
     * StoreException of internal data inconsistency
     */
    protected byte status;
    public Pair<Key,byte[]> next() throws IOException {
        buf.clear();
        long pos = OFFSET + (long) getCurRecord() * RECORD_SIZE;
        indexChannel.read(buf, pos);
        buf.rewind();
        byte status = buf.get();
        if ( isDeleted(status) ) {
            curRecord ++;
            return null;
        }
        else {
            long keyLen = buf.getLong();
            byte[] keys = readChannel( keyLen, keyChannel);
            long data = buf.getLong();
            long block2version = buf.getLong();
            CacheBlock block = new CacheBlock(curRecord, data, block2version, status );
            curRecord ++;
            if ( block.getDataOffset() <= 0 || block.getDataLen() <= 0 || block.getBlockSize() < block.getDataLen() ) {
                throw new StoreException("data reading error");
            }
            else {
                Key key = toKey(keys);
                byte[] datas = readChannel( block.getDataOffset2Len(), dataChannel);
                return new Pair( key, datas);
            }
        }
    }

    public KeyValue nextKeyValue() throws IOException {
        buf.clear();
        long pos = OFFSET + (long) getCurRecord() * RECORD_SIZE;
        indexChannel.read(buf, pos);
        buf.rewind();
        status = buf.get();
        if ( isDeleted(status) ) {
            curRecord ++;
            return null;
        }
        else {
            long keyLen = buf.getLong();
            byte[] keys = readChannel( keyLen, keyChannel);
            long data = buf.getLong();
            long block2version = buf.getLong();
            CacheBlock block = new CacheBlock(curRecord, data, block2version, status );
            curRecord ++;
            if ( block.getDataOffset() <= 0 || block.getDataLen() <= 0 || block.getBlockSize() < block.getDataLen() ) {
                throw new StoreException("data reading error");
            }
            else {
                Key key = toKey(keys);
                byte[] datas = readChannel( block.getDataOffset2Len(), dataChannel);
                return new KeyValue( key, createValue(datas, block.getVersion() , block.getNode()));
            }
        }
    }


    public void close() throws IOException {
        indexChannel.close();
        keyChannel.close();
        dataChannel.close();
    }


    protected boolean isDeleted(byte b) {
        if ( (b & DELETED) == DELETED ) return true;
        else return false;
    }

    public boolean deleteRecord() {
        if ( ! deleteFlag) {
            logger.warn("readonly mode, no delete support");
            return true;
        }
        try {
            long pos = OFFSET + (long) (getCurRecord()-1) * RECORD_SIZE;
            ByteBuffer buf = ByteBuffer.allocate(1);
            buf.put((byte) (status | DELETED));
            buf.flip();
            indexChannel.write(buf, pos);
            return true;
        } catch (IOException io) {
            logger.warn( io.getMessage() );
            return false;
        }
    }

    public int getTotalRecord() {
        return totalRecord;
    }

    public int getCurRecord() {
        return curRecord;
    }

    private boolean checkSignature(FileChannel channel) throws IOException {
        ByteBuffer intBytes = ByteBuffer.allocate(OFFSET);
        if ( channel.size() == 0) {
            throw new StoreException("File size is 0" );
        }
        else {
            channel.read(intBytes);
            intBytes.rewind();
            if ( intBytes.getInt() != MAGIC )
                throw new StoreException("Header mismatch expect "+MAGIC+" read "+ intBytes.getInt() );
        }
        return true;
    }

    protected byte[] readChannel(long offset2len, FileChannel channel)  throws IOException {
        long offset = getOffset( offset2len);
        int len = getLen( offset2len);
        ByteBuffer data = ByteBuffer.allocate( len);
        channel.read( data, offset);
        return data.array();
    }


}
