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
import voldemort.store.cachestore.CacheBlock;
import voldemort.store.cachestore.Key;
import voldemort.store.cachestore.Value;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.TimerTask;

import static voldemort.store.cachestore.BlockUtil.*;
import static voldemort.store.cachestore.impl.CacheStore.State;

/**
 * Created by IntelliJ IDEA.
 * User: mhsieh
 * Date: 3/3/11
 * Time: 11:50 AM
 * To change this template use File | Settings | File Templates.
 */
public class BackupThread extends TimerTask {
    private static Log logger = LogFactory.getLog(BackupThread.class);

    private CacheStore store;
    private int index;
    private String filename;
    private int threshold ;

    public BackupThread(CacheStore store, int index, String filename) {
        this( store, index, filename, 5*1024*1024 );

    }

    public BackupThread(CacheStore store, int index, String filename, int threshhold ) {
        this.store = store;
        this.index = index;
        this.filename = filename;
        this.threshold = threshhold;
    }

    public void run() {
        start( threshold );
    }

    public void start(int threshold) {
       if ( store.getServerState() != State.Active ) {
           logger.warn("Sever state is "+store.getServerState()+" can not run back up task");
           return;
       }
       logger.info("Start backup data to " + filename +" threshold " +threshold);
        try {
            store.setServerState(State.Backup);
            ChannelStore current = ChannelStore.open(filename, new HashMap<Key, Value>(), true, index, false);
            ChannelStore old = store.getList().get(index);
            int totalRecord = old.getTotalRecord();
            int error = 0 ;
            int deleted = 0;
            long per = 0; int j =0 ;
            per = totalRecord / 10 ;
            // monitor threshhold MB/sec
            long begin = System.currentTimeMillis();
            int bytesPerSecond = 0;
            logger.info("total records " + totalRecord );
            for ( int i =0 ; i < totalRecord ; i ++) {
                if( per > 0 &&  (i +1) % per == 0 ) {
                    logger.info( (++j * 10) + "% complete");
                }
                try {
                    Key key = old.readKey( i);
                    // key == null means delete flag is on
                    if ( key == null ) {
                        deleted ++;
                        continue;
                    }
                    CacheBlock block = store.getMap().get( key);
                    if ( block == null ) logger.info("key "+key.toString() +" is not longer in map");
                    else if ( block.getIndex() != index ) {
                        logger.info("skip, block.getIndex "+block.getIndex() + " not equal to " + index);
                    }
                    else {
                        synchronized( block) {
                           transfer( old, block, current, key, i);
                        }
                        if ( threshold > 0 ) {
                            bytesPerSecond += block.getDataLen();
                            if (bytesPerSecond > threshold ) {
                                long d = System.currentTimeMillis() - begin ;
                                // faster than thresh hold
                                if ( d < 1000 )  {
                                    try {
                                        Thread.sleep( 1000 - d);
                                    } catch (InterruptedException ex) {
                                        //swallow exception
                                    }
                                }
                                // reset parameter
                                begin = System.currentTimeMillis();
                                bytesPerSecond = 0 ;
                            }
                        }

                    }
                } catch (ArrayIndexOutOfBoundsException iex) {
                    logger.error( iex.getMessage()+" rec #"+i);
                    error++;
                } catch (Exception io) {
                    // swallow exception
                    logger.error(io.getMessage(), io);
                    error++;
                }

            }
            if ( error == 0 ) {
                logger.info("complete backup data " + filename +" total records "+totalRecord +" deleted "+deleted);
            } else  logger.error("backup data total " + error);
            if ( current != null ) {
                logger.info("close "+filename);
                current.close();
            }
        } catch ( Throwable th) {
            //swallow the error
            logger.error( th.getMessage(), th);
        } finally {
            store.setServerState(State.Active);
        }
    }

    private void transfer(ChannelStore old , CacheBlock block, ChannelStore current, Key key, int record) throws IOException {
        ByteBuffer from = null;
        // check to see read from channel
        if ( block.getData() == null )  {
            from = ByteBuffer.allocate(block.getDataLen());
            old.getDataChannel().read( from,  block.getDataOffset() );
        }
        byte[] keyBytes = toKeyBytes(key);
        long offset = current.getDataOffset();
        long keyOffset2Len = convertOffset4Len( current.getKeyOffset(), keyBytes.length);
        //!!! use rec instead record
        CacheBlock<byte[]> blk = makeBlock(current, current.getTotalRecord(), block.getBlockSize(),
                block.getDataLen(), block.getVersion(),  block.getNode());
        blk.setIndex(  store.getCurIndex());
        blk.setDirty( block.isDirty());
        blk.setDataOffset(offset);
        //blk.setRecordNo( rec);
        //write to data, key and index channel
        if ( block.getData() == null ) {
            from.flip();
            current.getDataChannel().write( from, offset );
        }
        else {
            current.getDataChannel().write(ByteBuffer.wrap( (byte[]) block.getData()) , offset );
        }
        if (blk.getBlockSize() > blk.getDataLen()  ) {
            ByteBuffer fillBuf = ByteBuffer.wrap( new byte[] {0} );
            current.getDataChannel().write( fillBuf, blk.getDataOffset()+ blk.getBlockSize()-1 );
        }
        current.getKeyChannel().write( ByteBuffer.wrap(keyBytes) ,getOffset(keyOffset2Len));
        current.writeIndexBlock(blk, keyOffset2Len, current.getIndexChannel() );
        current.setTotalRecord(current.getTotalRecord()+1);
        current.setDataOffset( blk.getDataOffset() + blk.getBlockSize());
        current.setKeyOffset( current.getKeyOffset() + keyBytes.length );
    }

    private CacheBlock makeBlock(ChannelStore channel,int record, int blockSize, int dataLen, long version, short node) {
        //long data = (dataOffset << LEN | dataLen) ;
        long data = convertOffset4Len(channel.getDataOffset(), dataLen) ;
        //checkFileSize(list.get(curIndex).getDataOffset(), dataLen);
        long b2v = convertVersion4Size(version, blockSize) ;
        return new CacheBlock( record, data, b2v, (byte) 0,  node );
    }
}
