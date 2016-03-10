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

package voldemort.store.cachestore;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: mhsieh
 * Date: 3/16/12
 * Time: 1:23 PM
 * To change this template use File | Settings | File Templates.
 */
public interface AccessQueue<Reference> {
    // remove next from header and set next and prev to null
    public Reference evictNextHead();
    // remove prev from tail and set next and prev to null
    public Reference evictNextTail();
    // add to tail
    public Reference add2Head(Reference ref);
    // add to header
    public Reference add2Tail(Reference ref);
    // move to head
    public boolean move2Head(Reference ref);
    // move to tail
    public boolean move2Tail(Reference ref);
    // size >= capacity
    public boolean isFull();
    // queue empty
    public boolean isEmpty();
    // capacity of queue
    public int getCapacity();
    // capacity - size
    public int remainingCapacity();
    // get all link from queue, don't apply the lock mainly for debug purpose
    public int size();
    public List<Reference> getAll();
}
