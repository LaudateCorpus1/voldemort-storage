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

import voldemort.store.cachestore.AccessQueue;
import voldemort.store.cachestore.CacheBlock;
import voldemort.store.cachestore.Reference;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by IntelliJ IDEA.
 * User: mhsieh
 * Date: 3/7/12
 * Time: 4:27 PM
 * To change this template use File | Settings | File Templates.
 */
public class DoubleLinkedList<T> implements AccessQueue<Reference>, Serializable {
   /** The capacity bound, or Integer.MAX_VALUE if none */
    private final int capacity;

    /** Current number of elements */
    private final AtomicInteger count = new AtomicInteger(0);
    //private final AtomicLong total = new AtomicLong(0);
    // head of linked list
    private volatile Reference<T> head;
    // tail of linked list
    private volatile Reference<T> tail;

    private Lock lock = new ReentrantLock(true);

    public DoubleLinkedList(int capacity) {
        this.capacity = capacity;
        this.head = new StrongReferenceImpl(null, null, null);
        this.tail = new StrongReferenceImpl(null, null, null);
        this.head.setNext( tail);
        this.tail.setPrev( head);
    }

    public boolean move2Tail(Reference ref) {
        lock.lock();
        try {
            if ( checkNull( ref )) return false;
            Reference pre = ref.getPrev();
            Reference next = ref.getNext();
            pre.setNext( next);
            next.setPrev( pre);
            Reference last = tail.getPrev();
            last.setNext( ref);
            ref.setPrev(last);
            ref.setNext(tail);
            tail.setPrev( ref);
            return true;
        }finally {
            lock.unlock();
        }

    }

    private boolean checkNull(Reference ref) {
        if ( ref == null || ref.getPrev() == null || ref.getNext() == null)
            return true;
        else
            return false;
    }

    public boolean move2Head(Reference ref ) {
        lock.lock();
          try {
              if ( checkNull( ref )) return false;
              Reference pre = ref.getPrev();
              Reference next = ref.getNext();
              pre.setNext( next);
              next.setPrev( pre);
              Reference first = head.getNext();
              first.setPrev(ref);
              ref.setNext(first);
              ref.setPrev( head);
              head.setNext( ref);
              return true;
          }finally {
              lock.unlock();
          }
    }

    public Reference add2Head(Reference ref) {
        lock.lock();
        try {
            Reference next = head.getNext();
            ref.setPrev( head);
            ref.setNext( next);
            next.setPrev( ref);
            head.setNext( ref);
            count.getAndIncrement();
            return ref;
        }finally {
            lock.unlock();
        }
    }

    public Reference add2Tail(Reference ref) {
       lock.lock();
        try {
            Reference prev = tail.getPrev();
            prev.setNext( ref);
            ref.setPrev( prev);
            ref.setNext( tail);
            tail.setPrev( ref);
            count.getAndIncrement();
            return ref;
        }finally {
            lock.unlock();
        }
    }


    public Reference evictNextHead() {
        if (lock.tryLock() ) {
            try {
                Reference ref = take4Head();
                if ( ref != null ) {
                    //synchronized (ref) {
                     //   ref.setData( null);
                        ref.setPrev(null);
                        ref.setNext(null);
                    //}
                }
                return ref;
            } finally {
                lock.unlock();
            }
        }
        else return null;

    }

    private Reference take4Head() {
        Reference ref = head.getNext();
        if ( ref.isDirty() ) return null;
        // check empty
        if ( ref == tail ) return null;
        Reference next = ref.getNext();
        head.setNext(next);
        next.setPrev( head);

        count.getAndDecrement();
        return ref;
    }

    public Reference takeHead() {
        lock.lock();
        try {
            return take4Head();

        } finally {
            lock.unlock();
        }
    }

    private Reference take4Tail(){
        Reference ref = tail.getPrev();
        if (  ref.isDirty() ) return null;
        if ( ref == head ) return null;
        Reference pre = ref.getPrev();
        tail.setPrev( pre);
        pre.setNext( tail);
        count.getAndDecrement();
        return ref;
    }

    public Reference takeTail() {
        lock.lock();
        try {
            return take4Head();
        } finally {
            lock.unlock();
        }
    }

    public Reference evictNextTail() {
        lock.lock();
        try {
            Reference ref = take4Tail();
            if ( ref != null ) {
                //synchronized (ref) {
                //    ref.setData( null);
                    ref.setPrev(null);
                    ref.setNext(null);
                //}
            }
            return ref;
        } finally {
            lock.unlock();
        }
    }

    public Reference getNextFromHead(){
        if ( capacity > count.get() ) {
            return takeHead();
        }
        else
            return null;
    }

    public Reference getNextFromTail() {
        if ( capacity > count.get() ) {
            return takeTail();
        }
        else
            return null;
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

    public Reference peekHead() {
        Reference ref = head.getNext();
        if ( ref == tail ) return null;
        else return ref;
    }

    public int size() {
        return count.get();
    }

    @Override
    public String toString() {
        return "capacity "+capacity+" size"+count.get();
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
        Reference prev = head;
        int i = 0;
        for (  Reference ref = head.getNext(); ref != null && ref != tail ; ref = ref.getNext() ) {
            if ( ref.getPrev() != prev ) {
                sb.append(" i "+i+" ,");
            }
            prev = ref ;
            i++;
        }
        sb.append(" end "+i+" diff "+(size  - i));
        return sb.toString();
    }

    public int remainingCapacity() {
        return capacity - count.get();
    }

    public void clear(){
        lock.lock();
        try {
            head.setNext(tail);
            head.setPrev(null);
            tail.setNext(null);
            tail.setPrev(head);
            count.set(0);
        }finally {
            lock.unlock();
        }
    }

    /**
     * It is not concurrently safe and for debug purpose only
     * since we will deal very large list, and did not apply lock
      * @return
     */
    public List<Reference> getAll() {
        List<Reference> list = new ArrayList<Reference>(count.get());
        for ( Reference ref = head.getNext(); ref != null && ref != tail ; ref = ref.getNext() ) {
            list.add( ref);
        }
        return list;
    }



}
