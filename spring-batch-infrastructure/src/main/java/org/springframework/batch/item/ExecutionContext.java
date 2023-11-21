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

package org.springframework.batch.item;

import org.springframework.util.Assert;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

/**
 * Object representing a context for an {@link ItemStream}. It is a thin
 * wrapper for a map that allows optionally for type safety on reads. It also
 * allows for dirty checking by setting a 'dirty' flag whenever any put is
 * called.
 *
 * @author Lucas Ward
 * @author Douglas Kaminsky
 */
public class ExecutionContext implements Serializable {

    private boolean dirty = false;

    private final Map<Object, Object> map;

    /**
     * Default constructor. Initializes a new execution context with an empty
     * internal map.
     */
    public ExecutionContext() {
        map = new HashMap<>();
    }

    /**
     * Initializes a new execution context with the contents of another map.
     *
     * @param map Initial contents of context.
     */
    public ExecutionContext(Map<Object, Object> map) {
        this.map = map;
    }

    /**
     * @param executionContext executionContext
     */
    public ExecutionContext(ExecutionContext executionContext) {
        this();
        if (executionContext == null) {
            return;
        }
        for (Entry<Object, Object> entry : executionContext.entrySet()) {
            this.map.put(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Adds a String value to the context.
     *
     * @param key   Key to add to context
     * @param value Value to associate with key
     */

    public void putString(String key, String value) {

        put(key, value);
    }

    /**
     * Adds a Long value to the context.
     *
     * @param key   Key to add to context
     * @param value Value to associate with key
     */
    public void putLong(String key, long value) {

        put(key, value);
    }

    /**
     * Add a Double value to the context.
     *
     * @param key   Key to add to context
     * @param value Value to associate with key
     */
    public void putDouble(String key, double value) {

        put(key, value);
    }

    /**
     * Add an Object value to the context (must be Serializable).
     *
     * @param key   Key to add to context
     * @param value Value to associate with key
     */
    public void put(String key, Object value) {
        if (value != null) {
            Assert.isInstanceOf(Serializable.class, value, "Value: [ " + value + "must be serializable.");
        }
        dirty = true;
        map.put(key, value);
    }

    /**
     * Indicates if context has been changed with a "put" operation since the
     * dirty flag was last cleared. Note that the last time the flag was cleared
     * might correspond to creation of the context.
     *
     * @return True if "put" operation has occurred since flag was last cleared
     */
    public boolean isDirty() {
        return dirty;
    }

    /**
     * Typesafe Getter for the String represented by the provided key.
     *
     * @param key The key to get a value for
     * @return The <code>String</code> value
     */
    public String getString(String key) {

        return (String) readAndValidate(key, String.class);
    }

    /**
     * Typesafe Getter for the String represented by the provided key with
     * default value to return if key is not represented.
     *
     * @param key           The key to get a value for
     * @param defaultString Default to return if key is not represented
     * @return The <code>String</code> value if key is repreesnted, specified
     * default otherwise
     */
    public String getString(String key, String defaultString) {
        if (!map.containsKey(key)) {
            return defaultString;
        }

        return (String) readAndValidate(key, String.class);
    }

    /**
     * Typesafe Getter for the Long represented by the provided key.
     *
     * @param key The key to get a value for
     * @return The <code>Long</code> value
     */
    public long getLong(String key) {

        return (Long) readAndValidate(key, Long.class);
    }

    /**
     * Typesafe Getter for the Long represented by the provided key with default
     * value to return if key is not represented.
     *
     * @param key         The key to get a value for
     * @param defaultLong Default to return if key is not represented
     * @return The <code>long</code> value if key is represented, specified
     * default otherwise
     */
    public long getLong(String key, long defaultLong) {
        if (!map.containsKey(key)) {
            return defaultLong;
        }

        return (Long) readAndValidate(key, Long.class);
    }

    /**
     * Typesafe Getter for the Double represented by the provided key.
     *
     * @param key The key to get a value for
     * @return The <code>Double</code> value
     */
    public double getDouble(String key) {
        return (Double) readAndValidate(key, Double.class);
    }

    /**
     * Typesafe Getter for the Double represented by the provided key with
     * default value to return if key is not represented.
     *
     * @param key           The key to get a value for
     * @param defaultDouble Default to return if key is not represented
     * @return The <code>double</code> value if key is represented, specified
     * default otherwise
     */
    public double getDouble(String key, double defaultDouble) {
        if (!map.containsKey(key)) {
            return defaultDouble;
        }

        return (Double) readAndValidate(key, Double.class);
    }

    /**
     * Getter for the value represented by the provided key.
     *
     * @param key The key to get a value for
     * @return The value represented by the given key
     */
    public Object get(String key) {

        return map.get(key);
    }

    /**
     * Utility method that attempts to take a value represented by a given key
     * and validate it as a member of the specified type.
     *
     * @param key  The key to validate a value for
     * @param type Class against which value should be validated
     * @return Value typed to the specified <code>Class</code>
     */
    private Object readAndValidate(String key, Class<?> type) {

        Object value = map.get(key);

        if (!type.isInstance(value)) {
            throw new ClassCastException("Value for key=[" + key + "] is not of type: [" + type + "], it is ["
                    + (value == null ? null : "(" + value.getClass() + ")" + value) + "]");
        }

        return value;
    }

    /**
     * Indicates whether or not the context is empty.
     *
     * @return True if the context has no entries, false otherwise.
     * @see java.util.Map#isEmpty()
     */
    public boolean isEmpty() {
        return map.isEmpty();
    }

    /**
     * Clears the dirty flag.
     */
    public void clearDirtyFlag() {
        dirty = false;
    }

    /**
     * Returns the entry set containing the contents of this context.
     *
     * @return A set representing the contents of the context
     * @see java.util.Map#entrySet()
     */
    public Set<Entry<Object, Object>> entrySet() {
        return map.entrySet();
    }

    /**
     * Indicates whether or not a key is represented in this context.
     *
     * @param key Key to check existence for
     * @return True if key is represented in context, false otherwise
     * @see java.util.Map#containsKey(Object)
     */
    public boolean containsKey(String key) {
        return map.containsKey(key);
    }

    /**
     * Indicates whether or not a value is represented in this context.
     *
     * @param value Value to check existence for
     * @return True if value is represented in context, false otherwise
     * @see java.util.Map#containsValue(Object)
     */
    public boolean containsValue(Object value) {
        return map.containsValue(value);
    }

    /**
     * Returns a <code>Properties</code> object containing <code>String</code>
     * versions of the contents of the context.
     *
     * @return Contents of context as a {@link java.util.Properties}
     * @deprecated to be removed with no replacement in 2.0. Should not be part
     * of public API (test purposes only)
     */
    public Properties getProperties() {
        Properties props = new Properties();
        for (Entry<Object, Object> entry : map.entrySet()) {
            props.setProperty(entry.getKey().toString(), entry.getValue().toString());
        }

        return props;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    public boolean equals(Object obj) {
        if (!(obj instanceof ExecutionContext)) {
            return false;
        }
        if (this == obj) {
            return true;
        }
        ExecutionContext rhs = (ExecutionContext) obj;
        return this.entrySet().equals(rhs.entrySet());
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
        return map.hashCode();
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    public String toString() {
        return map.toString();
    }

    /**
     * Returns number of entries in the context
     *
     * @return Number of entries in the context
     * @see java.util.Map#size()
     */
    public int size() {
        return map.size();
    }

}
