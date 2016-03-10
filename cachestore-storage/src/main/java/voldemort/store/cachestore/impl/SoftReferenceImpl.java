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

import voldemort.store.cachestore.Reference;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;

/**
 * Created by IntelliJ IDEA.
 * User: mhsieh
 * Date: 3/7/12
 * Time: 2:51 PM
 * To change this template use File | Settings | File Templates.
 */
public class SoftReferenceImpl<T> extends SoftReference implements Reference<T> {

    public SoftReferenceImpl(T referent) {
        super(referent);
}

    public SoftReferenceImpl(T referent, ReferenceQueue q) {
        super(referent, q);
    }

    @Override
    public T getData() {
        return (T) super.get();
    }

    @Override
    public void setData(T data) {
        // do nothing
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
        return this;
    }

    @Override
    public void setNext(Reference ref) {
        //To change body of implemented methods use File | Settings | File Templates.
    }


    @Override
    public void setPrev(Reference ref) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isDirty() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Reference getPrev() {
        return this;
    }
}
