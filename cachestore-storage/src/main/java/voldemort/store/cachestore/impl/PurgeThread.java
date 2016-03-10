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

import java.lang.reflect.Method;
import java.util.TimerTask;

import static voldemort.store.cachestore.impl.CacheStore.State;

/**
 * Created by IntelliJ IDEA.
 * User: mhsieh
 * Date: 3/7/11
 * Time: 12:31 PM
 * To change this template use File | Settings | File Templates.
 */
public class PurgeThread extends TimerTask {
    private static Log logger = LogFactory.getLog(PurgeThread.class);
    private Purge purge;
    private CacheStore store;

    public PurgeThread(Purge purge, CacheStore store) {
        this.purge = purge;
        this.store = store;
    }

    public void loadMaxDay() {
        try {
            Method method = purge.getClass().getDeclaredMethod("loadMaxDay", null);
            if ( method == null )
                logger.warn("can not find method loadMaxDay in "+purge.getClass().getName());
            else {
                int maxDays = (Integer) method.invoke(purge, null);
                logger.info("loading maxDaysBeforePurge " + maxDays);
            }
        } catch (Throwable ex) {
            //swallow exception
            logger.error("reload maxDaysBeforePurge fail", ex);
        }
    }
    @Override
    public void run() {
        loadMaxDay();
        store.getPackLock().lock();
        try {
            store.setServerState(State.Purge);
            int index = store.getCurIndex();
            ChannelStore current = store.getList().get(index);
            //int i = 0;
            //Iterator<Map.Entry<Key, CacheBlock>> iterator = store.getMap().entrySet().iterator();
            logger.info("index "+index+ " "+ ( current == null ? "current: null": current.toString()) );
            logger.info("Start to purge data ...");
            //Iterator <Key> keys = map.
            int totalRecord = current.getTotalRecord();
            int i = 0;
            int j = 0;
            int error = 0;
            int countFile = 0;
            long per = totalRecord / 10;
            for (int k = 0; k < totalRecord; k++) {
                if (per > 0 && (k + 1) % per == 0) {
                    logger.info((++j * 10) + "% complete deleted " + i + " error " + error);
                }
                try {
                    Key key = current.readKey(k);
                    // key == null means delete flag is on
                    if (key == null) {
                        continue;
                    }
                    CacheBlock block = store.getMap().get(key);
                    if (block == null) logger.info("key " + key.toString() + " is not in map " + block.toString());
                    else if (block.getIndex() != index) {
                        logger.info("skip, index " + block.getIndex() + " <> " + index + " " + block.toString());
                    } else {
                        synchronized (block) {
                            boolean inFile = false;
                            byte[] data = (byte[]) block.getData();
                            if (data == null) {
                                inFile = true;
                                countFile++;
                                data = current.readChannel(block.getDataOffset2Len(), current.getDataChannel());
                            } else {
                                // reduce the length if it is in memory
                                store.getCurrentMemory().getAndAdd(-data.length);
                            }
                            try {
                                if (purge.isDelete(data, key)) {
                                    store.remove(key);
                                    i++;
                                }
                            } catch (RuntimeException rex) {
                                logger.info(rex.getMessage());
                                logger.info("key " + key.toString() + " rec # " + k + " " + inFile + " " + block.toString());
                                store.remove(key);
                                i++;
                            }

                        }
                    }

                } catch (Exception io) {
                    // swallow exception
                    logger.error(io.getMessage(), io);
                    error++;

                }
            }
            int inMem = totalRecord - countFile;
            logger.info("total records purged " + i + " total record " + totalRecord + " in memory " + inMem + " % " + (inMem * 100 / totalRecord));
        } catch ( Exception ex) {

            logger.error( ex.getMessage(), ex);
        } finally {
            store.setServerState(State.Active);
            store.getPackLock().unlock();
        }
    }



}
