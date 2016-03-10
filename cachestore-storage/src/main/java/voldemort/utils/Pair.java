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

package voldemort.utils;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.io.Serializable;

/**
 * Represents a pair of items.
 */
public final class Pair<F, S> implements Serializable, Function<F, S> {

    private static final long serialVersionUID = 1L;

    private final F first;

    private final S second;

    /**
     * Static factory method that, unlike the constructor, performs generic
     * inference saving some typing. Use in the following way (for a pair of
     * Strings):
     * 
     * <p>
     * <code>
     * Pair<String, String> pair = Pair.create("first", "second");
     * </code>
     * </p>
     * 
     * @param <F> The type of the first thing.
     * @param <S> The type of the second thing
     * @param first The first thing
     * @param second The second thing
     * @return The pair (first,second)
     */
    public static final <F, S> Pair<F, S> create(F first, S second) {
        return new Pair<F, S>(first, second);
    }

    /**
     * Use the static factory method {@link #create(Object, Object)} instead of
     * this where possible.
     * 
     * @param first
     * @param second
     */
    public Pair(F first, S second) {
        this.first = first;
        this.second = second;
    }

    public S apply(F from) {
        if(from == null ? first == null : from.equals(first))
            return second;
        return null;
    }

    public final F getFirst() {
        return first;
    }

    public final S getSecond() {
        return second;
    }

    @Override
    public final int hashCode() {
        final int PRIME = 31;
        int result = 1;
        result = PRIME * result + ((first == null) ? 0 : first.hashCode());
        result = PRIME * result + ((second == null) ? 0 : second.hashCode());
        return result;
    }

    @Override
    public final boolean equals(Object obj) {
        if(this == obj)
            return true;
        if(!(obj instanceof Pair<?, ?>))
            return false;

        final Pair<?, ?> other = (Pair<?, ?>) (obj);
        return Objects.equal(first, other.first) && Objects.equal(second, other.second);
    }

    @Override
    public final String toString() {
        return new ToStringBuilder(this).append("first", first).append("second", second).toString();
    }
}
