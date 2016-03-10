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

import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * Created by IntelliJ IDEA.
 * User: mhsieh
 * Date: 1/25/11
 * Time: 11:28 AM
 * To change this template use File | Settings | File Templates.
 */
public interface BlockOps <T> {
    // persist a new block
    public void writeNewBlock(CacheBlock<T> block, long keyoffset2Len, byte[] key) throws IOException;
    // multiPuts a existing block
    public void writeExistBlock(CacheBlock<T> block) throws IOException;
    public void removeBlock(CacheBlock<T> block) throws IOException ;
    public void close();
    public byte[] readChannel(long offset2len, FileChannel cahnnel) throws IOException;
}
