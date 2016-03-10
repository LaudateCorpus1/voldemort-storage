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

package voldemort.store.cachestore.voldeimpl;

import voldemort.store.cachestore.Key;
import voldemort.store.cachestore.Value;

import java.io.Serializable;

/**
 * Created by IntelliJ IDEA.
 * User: mhsieh
 * Date: 4/13/12
 * Time: 10:59 AM
 * To change this template use File | Settings | File Templates.
 */
    public class KeyValue<V> implements Serializable {
        private Key key;
        private Value<V> value;

        public KeyValue(Key key, Value<V> value) {
            this.key = key;
            this.value = value;
        }

        public Key getKey() {
            return key;
        }

        public Value<V> getValue() {
            return value;
        }
    }
