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

/**
 * Created by IntelliJ IDEA.
 * User: mhsieh
 * Date: 1/25/11
 * Time: 11:33 AM
 * To change this template use File | Settings | File Templates.
 */
public interface Value <T> {
    // data is in byte array format
    public T getData();
    // set data
    public void setData(T data);
    // version info
    public long getVersion();
    public void setVersion(long version);
    // node info
    public short getNode() ;
    public void setNode(short node);

}
