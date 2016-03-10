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

/**
 * Created by IntelliJ IDEA.
 * User: mhsieh
 * Date: 2/23/11
 * Time: 2:53 PM
 * To change this template use File | Settings | File Templates.
 */
public class DelayBlock {
    private CacheBlock block;
    private Key key;
    private Object data;

    public DelayBlock(CacheBlock block, Key key) {
        this.block = block;
        this.key = key;
        if ( block.getData() != null ) {
            // force a strong reference
            data = block.getData();
        }
    }

    public CacheBlock getBlock() {
        return block;
    }

    public Key getKey() {
        return key;
    }
}
