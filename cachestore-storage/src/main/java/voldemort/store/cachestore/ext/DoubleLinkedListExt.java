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

package voldemort.store.cachestore.ext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import voldemort.store.cachestore.AccessQueue;
import voldemort.store.cachestore.Reference;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by IntelliJ IDEA.
 * User: mhsieh
 * Date: 3/16/12
 * Time: 11:58 AM
 * To change this template use File | Settings | File Templates.
 */

public class DoubleLinkedListExt<T> implements AccessQueue<Reference>, Serializable {
 private static Log logger = LogFactory.getLog(DoubleLinkedListExt.class);
    /** The capacity bound, or Integer.MAX_VALUE if none */
    private final int capacity;

    /** Current number of elements */
    private final AtomicInteger count = new AtomicInteger(0);
    //private final AtomicLong total = new AtomicLong(0);
    // header of linked list
    private volatile NodeRef header;
    // tail of linked list
    private volatile NodeRef tail;

    public DoubleLinkedListExt(int capacity) {
        this.capacity = capacity;
        header = new NodeRef(null, null, null);
        tail = new NodeRef(null,  null, header );
        header.setNext( tail);
    }

    public boolean move2Tail(Reference ref) {
        if ( checkNull(ref) ) return false;
        logger.info("try to delete "+ref.toString() );
        if ( ((NodeRef) ref).delete()) {
            return ((NodeRef) ref).move2Tail( tail);
            //return true;
        }
        else {
            logger.info("delete fail "+ref.toString() );
            return false;
        }
    }



    private boolean checkNull(Reference ref) {
        if ( ref == null || ref.getPrev() == null || ref.getNext() == null)
            return true;
        else
            return false;
    }

   public Reference addHead(T element) {
        while (true) {
            Reference ref = header.append( element );
            if ( ref != null ) {
                count.getAndIncrement();
                return ref;
            }
        }

    }

    public Reference addTail(T element) {
        while ( true ) {
            Reference ref = tail.prepend( element);
            if ( ref != null ) {
                count.getAndIncrement();
                return ref;
            }
        }
    }

    private static boolean usable(NodeRef n) {
        return n != null && !n.isSpecial();
    }

    public Reference evictNextHead() {
        while ( true) {
            Reference r = header.successor();
            if ( ! usable( (NodeRef) r))
                return null;
            if ( ! ((NodeRef) r).isDeleted()) {
                if ( ((NodeRef) r).delete() ) {
                    count.getAndDecrement();
                    return r;
                }
                else
                    return null;
            }
        }
    }



    public Reference evictNextTail() {
        while ( true) {
            Reference r = tail.predecessor();
            if ( ! usable( (NodeRef) r))
                return null;
            if ( ! ((NodeRef) r).isDeleted()) {
               if ( ((NodeRef) r).delete() ) {
                    count.getAndDecrement();
                    return r;
                }
                else
                    return null;
            }
        }
    }


    @Override
    public Reference add2Head(Reference ref) {
        return addHead((T) ref.getData());
    }

    @Override
    public Reference add2Tail(Reference ref) {
        return addTail((T) ref.getData());
    }

    @Override
    public boolean move2Head(Reference ref) {
        if ( checkNull(ref) ) return false;
        if ( ((NodeRef) ref).delete()) {
            return ((NodeRef) ref).move2Head(header);
            //return true;
        }
        else
            return false;
    }

    public boolean isFull() {
        return  count.get() > capacity ;
    }

    public boolean isEmpty() {
        return count.get() == 0 ;
    }

    public int getCapacity() {
        return capacity;
    }

    public int size() {
        return count.get();
    }

    public int remainingCapacity() {
        return capacity - count.get();
    }

    /**
     * It is not concurrently safe and for debug purpose only
     * since we will deal very large list, and did not apply lock
      * @return
     */
    public String validLink(){
        StringBuilder sb = new StringBuilder();
        int size = count.get();
        sb.append("size "+size );
        Reference prev = header;
        int i = 0;
        for (  Reference ref = header.getNext(); ref != null && ref != tail ; ref = ref.getNext() ) {
            if ( ref.getPrev() != prev ) {
                sb.append(" i "+i+" ,");
            }
            prev = ref ;
            i++;
        }
        sb.append(" end "+i+" diff "+(size  - i));
        return sb.toString();
    }

    @Override
    public List<Reference> getAll() {
        return null;
    }
}
