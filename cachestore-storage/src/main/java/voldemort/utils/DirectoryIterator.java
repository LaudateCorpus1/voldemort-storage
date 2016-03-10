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

import com.google.common.collect.AbstractIterator;

import java.io.File;
import java.util.Arrays;
import java.util.Stack;

/**
 * An iterator over all the files contained in a set of directories, including
 * any subdirectories
 * 
 * 
 */
//@NotThreadsafe
public class DirectoryIterator extends AbstractIterator<File> implements ClosableIterator<File> {

    private final Stack<File> stack;

    public DirectoryIterator(String... basis) {
        this.stack = new Stack<File>();
        for(String f: basis)
            stack.add(new File(f));
    }

    public DirectoryIterator(File... basis) {
        this.stack = new Stack<File>();
        stack.addAll(Arrays.asList(basis));
    }

    @Override
    protected File computeNext() {
        while(stack.size() > 0) {
            File f = stack.pop();
            if(f.isDirectory()) {
                for(File sub: f.listFiles())
                    stack.push(sub);
            } else {
                return f;
            }
        }
        return endOfData();
    }

    public void close() {}

    /**
     * Command line method to walk the directories provided on the command line
     * and print out their contents
     * 
     * @param args Directory names
     */
    public static void main(String[] args) {
        DirectoryIterator iter = new DirectoryIterator(args);
        while(iter.hasNext())
            System.out.println(iter.next().getAbsolutePath());
    }

}
