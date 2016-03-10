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

import java.io.Serializable;
import static voldemort.store.cachestore.BlockUtil.*;

/**
 * Created by IntelliJ IDEA.
 * User: mhsieh
 * Date: 1/25/11
 * Time: 11:30 AM
 * To change this template use File | Settings | File Templates.
 */
public class CacheBlock<T> implements Serializable, Reference<T> {

  // the first 4 bits for channel - max 16 channel
    // last 2 bits for status, delete bit is last bit
    private byte status;
    // record for index file it is 0 based
    private int recordNo ;
    // value
    private volatile Reference value;
    // dataBlock offset(40bits) and len (24 bits)
    private long dataOffset2Len;
    // blocksize (24 bits) and version (40bits)
    private long block2Version ;
    // add node
    private short node;

    public CacheBlock(int recordNo, long dataOffset2Len, long block2Version, byte status, short node) {
        this.recordNo = recordNo;
        this.block2Version = block2Version;
        this.dataOffset2Len = dataOffset2Len;
        this.status = status ;
        this.node = node;
    }

    public CacheBlock(int recordNo, long dataOffset2Len, long block2Version, byte status) {
        this(recordNo, dataOffset2Len, block2Version, status, (short) 0 );
    }


    public CacheBlock(int recordNo, long dataOffset2Len, long block2Version) {
        this(recordNo, dataOffset2Len, block2Version,(byte) 0, (short) 0 );
    }


    public Reference getValue() {
        return value;
    }

    public short getNode() {
        return node;
    }

    public void setNode(short node) {
        this.node = node;
    }

    public long getDataOffset() {
        return getOffset( dataOffset2Len)  ;
    }

    public int getDataLen() {
        return getLen(dataOffset2Len) ;
    }

    public long getVersion() {
        return getVersionNo(block2Version);
    }

    public int getBlockSize() {
        return getSize(block2Version);
    }

    public void setDataOffset(long offset) {

       dataOffset2Len = convertOffset4Len( offset ,getLen(dataOffset2Len) ) ;
    }

    public void setDataLen(int len) {
       dataOffset2Len =  convertOffset4Len( getOffset( dataOffset2Len) , len) ;
    }

    public void setBlockSize(int size) {
        block2Version =convertVersion4Size ( getVersionNo( block2Version) , size) ;
    }

    public void setVersion(long version) {
        block2Version = convertVersion4Size( version, getSize(block2Version)) ;
    }

    public T getData() {
        if (value == null ) return null;
        else
            return (T) value.getData() ;
    }

    @Override
    public void setData(T data) {
        if ( value != null ) value.setData( data);
        else {
            throw new RuntimeException("can not set, value is null");
        }
    }

    public void setDataNull() {
        if ( value != null ) {
            value.setData( null);
        }
    }

    @Override
    public long getAccessTime() {
        return value.getAccessTime();
    }

    @Override
    public void setAccessTime(long time) {
        value.setAccessTime( time);
    }

    @Override
    public Reference getNext() {
        return value.getNext();
    }

    @Override
    public void setNext(Reference ref) {
        value.setNext( ref);
    }

    @Override
    public Reference getPrev() {
        return value.getPrev();
    }

    @Override
    public void setPrev(Reference ref) {
        value.setPrev( ref);
    }

    public int getRecordNo() {
        return recordNo;
    }

    public void setRecordNo(int recordNo) {
        this.recordNo = recordNo;
    }

    public byte getStatus() {
        return status;
    }

    public void setStatus(byte status) {
        this.status = status;
    }

    public long getDataOffset2Len() {
        return dataOffset2Len;
    }

    public long getBlock2Version() {
        return block2Version ;
    }

    public boolean isDelete() {
        if ( (status & DELETED) == DELETED ) return true;
        else return false;
    }

    public void setDelete(boolean deleted) {
        if ( deleted ) status = (byte) (status | DELETED );
        else status = (byte) (status | 0 ) ;
    }

    public int getIndex() {
        return ( ( status & 0xF0 ) >>> STATUS_BIT) ;
    }

    public void  setIndex(int i) {
        status = (byte) ( ( i << STATUS_BIT ) | ( 0x0F & status) ) ;
    }

    public boolean isDirty() {
        if ( (status & DIRTY) == DIRTY ) return true;
        else return false;

    }

    public void setDirty(boolean dirty) {
        if ( dirty ) status = (byte) (status | DIRTY );
        else status = (byte) (status & NOT_DIRTY );
    }

    public void setValue(Reference value){
        this.value = value;
    }

    @Override
    public String toString() {
        return "# "+getRecordNo()+" idx "+getIndex()+" oft "+getDataOffset()+" len "+getDataLen()+" size "+getBlockSize()+" "+isDirty();
    }
}

