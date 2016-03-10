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
import voldemort.store.cachestore.Key;
import voldemort.store.cachestore.Value;
import voldemort.store.cachestore.impl.CacheStore;
import voldemort.store.cachestore.impl.LogChannel;

import java.util.concurrent.LinkedBlockingQueue;

import static voldemort.store.cachestore.voldeimpl.VoldeUtil.*;

/**
 * Created by IntelliJ IDEA.
 * User: mhsieh
 * Date: 4/13/12
 * Time: 9:53 AM
 * To change this template use File | Settings | File Templates.
 */
public class WriteLog implements Runnable {
    private static Log logger = LogFactory.getLog(WriteLog.class);

    private String logPath;
    private LinkedBlockingQueue<KeyValue> logQueue;
    private CacheStore trxStore;
    private String storeName;
    private LogChannel logChannel;
    private Value<byte[]> index;
    private Key indexKey;

    public WriteLog(String logPath, LinkedBlockingQueue<KeyValue> logQueue, CacheStore trxStore, String storeName) {
        this.logPath = logPath;
        this.logQueue = logQueue;
        this.trxStore = trxStore;
        this.storeName = storeName;
        this.indexKey = Key.createKey(storeName+POSTFIX);
        init();
    }

    private void init() {
        if ( trxStore.get( indexKey) == null) {
            index = toValue(0, 0);
            trxStore.put(indexKey, index );
        }
        else
            index = (Value<byte[]>) trxStore.get( indexKey);

        logger.info("index "+toInt(index));
        logChannel =  createLogChannel(toInt( index));
    }

    private LogChannel createLogChannel(int no) {
        return  new LogChannel(storeName+"."+no, no, logPath);
    }

    public Value<byte[]> getIndex() {
        return index;
    }

    public LogChannel getLogChannel() {
        return logChannel;
    }

    public CacheStore getTrxStore() {
        return trxStore;
    }

    public String getStoreName() {
        return storeName;
    }

    public LinkedBlockingQueue<KeyValue> getLogQueue() {
        return logQueue;
    }

    public String getLogPath() {
        return logPath;
    }

    @Override
    public String toString() {
        return ( logChannel == null ) ? "N/A" : logChannel.toString();
    }

    @Override
    public void run() {
            while ( true) {
                try {
                    KeyValue keyValue = logQueue.take();
                    // delete operation if value == null
                    if ( keyValue.getValue() == null)
                        logChannel.writeDelete( keyValue.getKey() );
                    else
                        logChannel.writeRecord( keyValue.getKey(), keyValue.getValue() );

                    if ( logChannel.getTotalRecord() >= MAX_RECORD ) {
                        int n = toInt( index )+ 1 ;
                        logger.info("create a new log channel "+n);
                        logChannel.close();
                        //int n = toInt( index );
                        index = toValue( n, 0);
                        logChannel = createLogChannel(toInt(index));
                        trxStore.put(indexKey, index);
                        // pass to replicate client
                    }
                } catch (Throwable th) {
                    logger.error( th.getMessage(), th);
                }
            }
    }
}
