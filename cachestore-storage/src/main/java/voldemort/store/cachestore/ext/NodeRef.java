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

import voldemort.store.cachestore.Reference;

import java.util.concurrent.atomic.AtomicReference;

/**
 * This code base origially written by Doug Lea in ConcurrentDoubleLinkedList
   and implement Reference to support cachestore
 */
public class NodeRef<E> extends AtomicReference<Reference<E>> implements Reference<E> {
    private volatile Reference<E> prev;

    private E element;



  /** Creates a node with given contents */
  NodeRef(E element, Reference<E> next, Reference<E> prev) {
    super(next);
    this.prev = prev;
    this.element = element;
  }

  /** Creates a marker node with given successor */
  NodeRef(Reference<E> next) {
    super(next);
    this.prev = this;
    this.element = null;
  }

  /**
   * Gets next link (which is actually the value held as atomic
   * reference).
   */
  public Reference<E> getNext() {
    return get();
  }

    @Override
    public void setNext(Reference ref) {
        set(ref);
        //To change body of implemented methods use File | Settings | File Templates.
    }


    /**
   * Sets next link
   *
   * @param n
   *            the next node
   */
//  //@Override
//  public void setNext(Reference<E> n) {
//    set(n);
//  }

  /**
   * compareAndSet next link
   */
  private boolean casNext(NodeRef<E> cmp, NodeRef<E> val) {
    return compareAndSet(cmp, val);
  }

  /**
   * Gets prev link
   */
  public Reference<E> getPrev() {
    return prev;
  }

    @Override
    public void setPrev(Reference ref) {
        this.prev = ref;
    }

    @Override
    public boolean isDirty() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }


    /**
   * Returns true if this is a header, trailer, or marker node
   */
  boolean isSpecial() {
    return element == null;
  }

  /**
   * Returns true if this is a trailer node
   */
  boolean isTrailer() {
    return getNext() == null;
  }

  /**
   * Returns true if this is a header node
   */
  boolean isHeader() {
    return getPrev() == null;
  }

  /**
   * Returns true if this is a marker node
   */
  boolean isMarker() {
    return getPrev() == this;
  }

  /**
   * Returns true if this node is followed by a marker, meaning that it is
   * deleted.
   *
   * @return true if this node is deleted
   */
  boolean isDeleted() {
    Reference<E> f = getNext();
    return f != null && ((NodeRef)f).isMarker();
  }

  /**
   * Returns next node, ignoring deletion marker
   */
  private Reference<E> nextNonmarker() {
    Reference<E> f = getNext();
    return (f == null || !((NodeRef)f).isMarker()) ? f : f.getNext();
  }

  /**
   * Returns the next non-deleted node, swinging next pointer around any
   * encountered deleted nodes, and also patching up successor''s prev
   * link to point back to this. Returns null if this node is trailer so
   * has no successor.
   *
   * @return successor, or null if no such
   */
  Reference<E> successor() {
    Reference<E> f = nextNonmarker();
    for (;;) {
      if (f == null)
        return null;
      if (!((NodeRef)f).isDeleted()) {
        if (f.getPrev() != this && !isDeleted())
          f.setPrev(this); // relink f's prev
        return f;
      }
      Reference<E> s = ((NodeRef)f).nextNonmarker();
      if (f == getNext())
        casNext((NodeRef) f, (NodeRef) s); // unlink f
      f = s;
    }
  }

  /**
   * Returns the apparent predecessor of target by searching forward for
   * it starting at this node, patching up pointers while traversing. Used
   * by predecessor().
   *
   * @return target's predecessor, or null if not found
   */
  private Reference<E> findPredecessorOf(NodeRef<E> target) {
    NodeRef<E> n = this;
    for (;;) {
      Reference<E> f = n.successor();
      if (f == target)
        return n;
      if (f == null)
        return null;
      n = (NodeRef)f;
    }
  }

  /**
   * Returns the previous non-deleted node, patching up pointers as
   * needed. Returns null if this node is header so has no successor. May
   * also return null if this node is deleted, so doesn't have a distinct
   * predecessor.
   *
   * @return predecessor or null if not found
   */
  Reference<E> predecessor() {
    Reference<E> n = this;
    for (;;) {
      Reference<E> b = n.getPrev();
      if (b == null)
        return ((NodeRef)n).findPredecessorOf(this);
      Reference<E> s = b.getNext();
      if (s == this)
        return b;
      if (s == null || ! ((NodeRef)s).isMarker()) {
        Reference<E> p = ((NodeRef) b).findPredecessorOf(this);
        if (p != null)
          return p;
      }
      n = b;
    }
  }

  /**
   * Returns the next node containing a nondeleted user element. Use for
   * forward list traversal.
   *
   * @return successor, or null if no such
   */
  Reference<E> forward() {
    Reference<E> f = successor();
    return (f == null || ((NodeRef)f).isSpecial()) ? null : f;
  }

  /**
   * Returns previous node containing a nondeleted user element, if
   * possible. Use for backward list traversal, but beware that if this
   * method is called from a deleted node, it might not be able to
   * determine a usable predecessor.
   *
   * @return predecessor, or null if no such could be found
   */
  NodeRef<E> back() {
    NodeRef<E> f = (NodeRef) predecessor();
    return (f == null || f.isSpecial()) ? null : f;
  }

  /**
   * Tries to insert a node holding element as successor, failing if this
   * node is deleted.
   *
   * @param element
   *            the element
   * @return the new node, or null on failure.
   */
  Reference<E> append(E element) {
    for (;;) {
      Reference<E> f = getNext();
      if (f == null || ((NodeRef)f).isMarker())
        return null;
      NodeRef<E> x = new NodeRef<E>(element, f, this);
      if (casNext(((NodeRef)f), x)) {
        f.setPrev(x); // optimistically link
        return x;
      }
    }
  }

  /**
   * Tries to insert a node holding element as predecessor, failing if no
   * live predecessor can be found to link to.
   *
   * @param element
   *            the element
   * @return the new node, or null on failure.
   */
  Reference<E> prepend(E element) {
    for (;;) {
      Reference<E> b = predecessor();
      if (b == null)
        return null;
      NodeRef<E> x = new NodeRef<E>(element, this, b);
      if (((NodeRef)b).casNext(this, x)) {
        setPrev(x); // optimistically link
        return x;
      }
    }
  }

  /**
   * Tries to mark this node as deleted, failing if already deleted or if
   * this node is header or trailer
   *
   * @return true if successful
   */
  boolean delete() {
    Reference<E> b = getPrev();
    Reference<E> f = getNext();
    if (b != null && f != null && !((NodeRef)f).isMarker()
        && casNext(((NodeRef)f), new NodeRef(f))) {
      if (((NodeRef)b).casNext(this, (NodeRef)f))
        f.setPrev(b);
      return true;
    }
    return false;
  }

    /**
     *
      * @param ref must be tail
     * @return
     */
  boolean move2Tail(Reference<E> ref) {
    Reference<E> b = ref.getPrev();
    Reference<E> c = b.getNext();
    Reference<E> f = this.getNext();
     if (b != null && c != null && !((NodeRef)b).isMarker()
        && (casNext( ( NodeRef) f , (NodeRef) ref ) )) {
      ref.setPrev(this);
      if (((NodeRef)b).casNext((NodeRef)ref , this))
        this.setPrev(b);
      return true;
    }
    return false;
  }

  boolean move2Head(Reference ref) {
    Reference<E> f = ref.getNext();
    Reference<E> c = f.getNext();
    Reference<E> b = this.getNext();
     if (c != null && f != null && !((NodeRef)f).isMarker()
        && ( ((NodeRef) ref).casNext( ( NodeRef) f , this ) )) {
      ref.setPrev(this);
      if (((NodeRef) this).casNext((NodeRef) b , (NodeRef) f))
        this.setPrev(f);
      return true;
    }
    return false;
  }

  /**
   * Tries to insert a node holding element to replace this node. failing
   * if already deleted.
   *
   * @param newElement
   *            the new element
   * @return the new node, or null on failure.
   */
  Reference<E> replace(E newElement) {
    for (;;) {
      Reference<E> b = getPrev();
      Reference<E> f = getNext();
      if (b == null || f == null || (( NodeRef)f).isMarker())
        return null;
      Reference<E> x = new NodeRef<E>(newElement, f, b);
      if (casNext(((NodeRef)f), new NodeRef(x))) {
        ((NodeRef)b).successor(); // to relink b
        ((NodeRef)x).successor(); // to relink f
        return x;
      }
    }
  }

    @Override
    public String toString() {
        return  element == null ? "null" : element.toString() ;
    }

    @Override
    public E getData() {
        return element;  //To change body of implemented methods use File | Settings | File Templates.
    }


    @Override
    public void setData(E data) {
        element = data;
    }

    @Override
    public long getAccessTime() {
        return 0;
    }

    @Override
    public void setAccessTime(long time) {
    }


}
