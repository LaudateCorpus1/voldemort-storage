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
import voldemort.store.cachestore.CacheBlock;
import voldemort.store.cachestore.Key;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.TimerTask;

import static voldemort.store.cachestore.BlockUtil.convertOffset4Len;
import static voldemort.store.cachestore.BlockUtil.getOffset;
import static voldemort.store.cachestore.BlockUtil.toKeyBytes;
import static voldemort.store.cachestore.impl.CacheStore.State;

/**
 * pack thread, will purge overflow and deleted block, and move current active data to different channel
 * the trigger conditions could be reached by the overflow size > trigger size
 * or user could issue pack command
 * only user call pack() to activate it
 */

public class PackThread extends TimerTask {
    private static Log logger = LogFactory.getLog(PackThread.class);

    private CacheStore store;
    // force pack data it is triggered by user
    private boolean force;
    private int threshold;

    public PackThread(CacheStore store, boolean force, int threshhold) {
        this.store = store;
        this.force = force;
        if ( threshhold < 1024 )
            this.threshold = threshhold * 1024* 1024;
        else
            this.threshold = 0;
    }

    public PackThread(CacheStore store, boolean force) {
        this(store, force, 0);
    }

    public void run() {
        if ( ! force ) {
            if ( store.getOverflow().get() < store.getTrigger()) {
                logger.info("overflow "+ store.getOverflow()+" is below trigger "+ store.getTrigger() );// skip
                return;
            }
            else {
                if ( store.getStatList() != null)
                    store.getStatList().set( 0, System.currentTimeMillis());
            }
        }
        int index = store.getCurIndex();
        logger.info("Start packing data for channel index " + index );
        // reset overflow
        store.getOverflow().getAndSet( 0L);
        store.setDeleted( 0);
        store.getPackLock().lock();
        try {
            store.setServerState(State.Pack);
            int k = store.createChannel(index, store.getNamePrefix() );
            store.setCurIndex(k);
            ChannelStore old = store.getList().get(index);
            // set curIndex after create new channel for data
            int error = 0 ; int deleted =0 ;
            int totalRecord = old.getTotalRecord();
            long per = 0; int j =0 ;
            per = totalRecord / 10 ;
            logger.info( k + " curIndex channel status "+ store.getList().get(index).getDataChannel().isOpen()+" total record "+totalRecord );
            // monitor threshold MB/sec
            long begin = System.currentTimeMillis();
            int bytesPerSecond = 0;
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
                    if ( block == null ) logger.info("key "+key.toString() +" is not in map "+block.toString());
                    else if ( block.getIndex() != index ) {
                        logger.info("skip, Index "+block.getIndex() + " <> " + index+ " "+block.toString()+" rec #"+i+" key "+key.toString());
                    }
                    else {
                        synchronized( block) {
                           transfer( old, block, store.getList().get(k), key);
                        }
                        if ( threshold > 0 ) {
                            bytesPerSecond += block.getDataLen();
                            if (bytesPerSecond > threshold) {
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

                } catch (Exception io) {
                    // swallow exception
                    logger.error("rec #"+i+" "+io.getMessage(), io);
                    error++;
                }
            } //for

            if ( error > 0 )
                logger.error("pack data total " + error);
            logger.info("complete packing data for channel index " + index +" total record "+totalRecord+
                    " deleted "+deleted);
            store.removeChannels(index);
        } catch ( Throwable th) {
            //swallow the error
            logger.error( th.getMessage(), th);
        } finally {
            store.setServerState(State.Active);
            // multiPuts stat for back groud task
            if ( ! force && store.getStatList() != null ) store.getStatList().set(1, System.currentTimeMillis());
            store.getPackLock().unlock();
        }
    }


    private void transfer(ChannelStore old , CacheBlock block, ChannelStore current, Key key) throws IOException {
        // new implementation to take care of currency issue
        ByteBuffer from = null;
        // check to see read from channel
        if ( block.getData() == null )  {
            from = ByteBuffer.allocate(block.getDataLen());
            old.getDataChannel().read( from,  block.getDataOffset() );
        }
        byte[] keyBytes = toKeyBytes(key);
        long offset ;
        long keyOffset2Len ;
        int record ;
        store.getLock().lock();
        try {
            offset = current.getDataOffset();
            record = current.getTotalRecord();
            keyOffset2Len = convertOffset4Len( current.getKeyOffset(), keyBytes.length);
            // multiPuts current channel position
            current.setTotalRecord(record +1);
            current.setDataOffset(offset + block.getBlockSize());
            current.setKeyOffset(current.getKeyOffset() + keyBytes.length );
        } finally {
            store.getLock().unlock();
        }
        block.setIndex( store.getCurIndex());
        block.setDataOffset(offset);
        block.setRecordNo( record );
        block.setDirty(false);
        //write to data, key and index channel
        if ( block.getData() == null ) {
            from.flip();
            current.getDataChannel().write( from, offset );
        }
        else {
            current.getDataChannel().write(ByteBuffer.wrap( (byte[]) block.getData()) , offset );
        }
        if (block.getBlockSize() > block.getDataLen()  ) {
            ByteBuffer fillBuf = ByteBuffer.wrap( new byte[] {0} );
            current.getDataChannel().write( fillBuf, block.getDataOffset()+ block.getBlockSize()-1 );
        }
        current.getKeyChannel().write( ByteBuffer.wrap(keyBytes) ,getOffset(keyOffset2Len));
        current.writeIndexBlock(block, keyOffset2Len, current.getIndexChannel() );
    }


}

