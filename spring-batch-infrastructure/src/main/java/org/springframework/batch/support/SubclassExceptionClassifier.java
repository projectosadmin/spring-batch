/*
 * Copyright 2006-2007 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.batch.support;

import org.springframework.util.Assert;

import java.util.*;

/**
 * @author Dave Syer
 */
public class SubclassExceptionClassifier extends ExceptionClassifierSupport {

    private Map<Class<?>, Object> classified = new HashMap<>();

    /**
     * Map of Throwable class types to keys for the classifier. Any subclass of
     * the type provided will be classified as of the type given by the
     * corresponding map entry value.
     *
     * @param typeMap the typeMap to set
     */
    public final void setTypeMap(Map<Class<?>, Object> typeMap) {
        Map<Class<?>, Object> map = new HashMap<>();
        for (Map.Entry<Class<?>, Object> entry : typeMap.entrySet()) {
            addRetryableExceptionClass(entry.getKey(), entry.getValue(), map);
        }
        this.classified = map;
    }

    /**
     * Return the value from the type map whose key is the class of the given
     * Throwable, or its nearest ancestor if a subclass.
     *
     * @see org.springframework.batch.support.ExceptionClassifierSupport#classify(java.lang.Throwable)
     */
    public Object classify(Throwable throwable) {

        if (throwable == null) {
            return super.classify(null);
        }

        Class<? extends Throwable> exceptionClass = throwable.getClass();
        if (classified.containsKey(exceptionClass)) {
            return classified.get(exceptionClass);
        }

        // check for subclasses
        Set<Class<?>> classes = new TreeSet<>(new ClassComparator());
        classes.addAll(classified.keySet());
        for (Class<?> aClass : classes) {
            if (aClass.isAssignableFrom(exceptionClass)) {
                Object value = classified.get(aClass);
                addRetryableExceptionClass(exceptionClass, value, this.classified);
                return value;
            }
        }

        return super.classify(throwable);
    }

    private void addRetryableExceptionClass(Class<?> candidateClass, Object classifiedAs, Map<Class<?>, Object> map) {
        //Assert.isAssignable(Class.class, candidateClass.getClass());
        Assert.isAssignable(Throwable.class, candidateClass);
        map.put(candidateClass, classifiedAs);
    }

    /**
     * Comparator for classes to order by inheritance.
     *
     * @author Dave Syer
     */
    private static class ClassComparator implements Comparator<Class<?>> {
        /**
         * @return 1 if arg0 is assignable from arg1, -1 otherwise
         * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
         */
        public int compare(Class<?> arg0, Class<?> arg1) {
            if (arg0 == arg1) {
                return 0;
            }
            if (arg0.isAssignableFrom(arg1)) {
                return 1;
            }
            return -1;
        }
    }

}
