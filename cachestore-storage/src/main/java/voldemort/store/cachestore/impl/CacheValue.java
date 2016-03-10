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
import voldemort.store.cachestore.StoreException;
import voldemort.store.cachestore.Value;

import java.io.Serializable;

import static voldemort.store.cachestore.BlockUtil.* ;

/**
 * Created by IntelliJ IDEA.
 * User: derek
 * Date: May 15, 2010
 * Time: 8:14:23 AM
 *
 */

public final class CacheValue implements Value<byte[]> , Serializable {
    // data must be in byte[] format
    private byte[] data;
    // 20 bits for node info including partition info, 44 bits using for version
    // refer to BlockUtil
    private long node2version;

    private CacheValue(byte[] data, long node2version) {
        this.data = data;
        this.node2version = node2version;
    }

    /**
     *  set version to 0, iskip checking version on server side
     *  @param data
     * @return
     */
    public static CacheValue createValue(byte[] data) {
        return new CacheValue( data, 0);
    }

    public static CacheValue createValue(byte[] data, long version) {
        if ( version >  VERSION_MASK ) throw new StoreException("Version exceeding "+VERSION_MASK);
        return new CacheValue( data, convertVersion4Size( version, 0));
    }

    public static CacheValue createValue(byte[] data, long version, short node) {
        if ( version >  VERSION_MASK ) throw new StoreException("Version exceeding "+VERSION_MASK);
        return new CacheValue( data, convertVersion4Size( version, node));
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public long getNode2version() {
        return node2version;
    }

    public long getVersion() {
        return getVersionNo( node2version);
    }

    public short getNode() {
        return (short) getSize( node2version);
    }


    public long increaseVersion() {
        long version = getVersionNo( node2version) +1  ;
        if ( version >  VERSION_MASK ) throw new StoreException("Version exceeding "+VERSION_MASK);
        node2version = convertVersion4Size( version, getSize( node2version));
        return version;
    }

    public void setNode(short node) {
        node2version = convertVersion4Size( getVersionNo( node2version), node);
    }

    public void setVersion(long version) {
        if ( version >  VERSION_MASK ) throw new StoreException("Version exceeding "+VERSION_MASK);
        node2version = convertVersion4Size( version, getSize( node2version));
    }
}

