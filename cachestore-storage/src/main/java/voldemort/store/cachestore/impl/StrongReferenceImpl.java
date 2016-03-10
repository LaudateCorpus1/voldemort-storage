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
import voldemort.store.cachestore.Reference;

/**
 * Created by IntelliJ IDEA.
 * User: mhsieh
 * Date: 3/7/12
 * Time: 3:03 PM
 * To change this template use File | Settings | File Templates.
 */
public class StrongReferenceImpl<T> implements Reference<T> {
    private T data;
    private volatile Reference prev;
    private volatile Reference next;

    public StrongReferenceImpl(T data) {
        this(data, null, null);
    }

    public StrongReferenceImpl(T data, Reference pre, Reference next) {
        this.data = data;
        this.prev = pre;
        this.next = next;
    }

    @Override
    public T getData() {
        return data;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setData(T data) {
        this.data = data;
    }



    @Override
    public long getAccessTime() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setAccessTime(long time) {
    }

    @Override
    public Reference getNext() {
        return next;
    }

    @Override
    public void setNext(Reference ref) {
        next = ref;
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Reference getPrev() {
        return prev;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setPrev(Reference ref) {
        this.prev = ref;
    }

    @Override
    public boolean isDirty() {
        return ((CacheBlock) data).isDirty();
    }
}
