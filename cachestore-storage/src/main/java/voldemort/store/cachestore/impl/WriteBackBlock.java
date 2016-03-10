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

import voldemort.store.cachestore.CacheBlock;
import voldemort.store.cachestore.Key;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;

/**
 * Created by IntelliJ IDEA.
 * User: mhsieh
 * Date: 1/26/11
 * Time: 7:06 PM
 * Support delay write back, intercept GC and check isDirty() to persist the data
 */
public class WriteBackBlock<T> extends SoftReference  {
    private final Key key;
    private final CacheBlock block;


    public WriteBackBlock(T object, ReferenceQueue referenceQueue, CacheBlock block, Key key) {
        super(object, referenceQueue);
        this.block = block;
        this.key = key;

    }

    public Key getKey() {
        return key;
    }

    public CacheBlock getBlock() {
        return block;
    }
}
