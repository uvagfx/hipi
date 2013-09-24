/**
 * Copyright 2009 Rednaxela
 * 
 * This software is provided 'as-is', without any express or implied
 * warranty. In no event will the authors be held liable for any damages
 * arising from the use of this software.
 * 
 * Permission is granted to anyone to use this software for any purpose,
 * including commercial applications, and to alter it and redistribute it
 * freely, subject to the following restrictions:
 * 
 *    1. The origin of this software must not be misrepresented; you must not
 *    claim that you wrote the original software. If you use this software
 *    in a product, an acknowledgment in the product documentation would be
 *    appreciated but is not required.
 * 
 *    2. This notice may not be removed or altered from any source
 *    distribution.
 */

package hipi.util.kdtree;

import java.util.Arrays;

/**
 * An implementation of an implicit binary heap. Min-heap and max-heap both supported 
 */
public abstract class BinaryHeap<T> {
    protected static final int defaultCapacity = 64;
    private final int direction;
    private Object[] data;
    private float[] keys;
    private int capacity;
    private int size;

    protected BinaryHeap(int capacity, int direction) {
        this.direction = direction;
        this.data = new Object[capacity];
        this.keys = new float[capacity];
        this.capacity = capacity;
        this.size = 0;
    }

    public void offer(float key, T value) {
        // If move room is needed, float array size
        if (size >= capacity) {
            capacity *= 2;
            data = Arrays.copyOf(data, capacity);
            keys = Arrays.copyOf(keys, capacity);
        }

        // Insert new value at the end
        data[size] = value;
        keys[size] = key;
        siftUp(size);
        size++;
    }

    protected void removeTip() {
        if (size == 0) {
            throw new IllegalStateException();
        }

        size--;
        data[0] = data[size];
        keys[0] = keys[size];
        data[size] = null;
        siftDown(0);
    }
    
    protected void replaceTip(float key, T value) {
        if (size == 0) {
            throw new IllegalStateException();
        }

        data[0] = value;
        keys[0] = key;
        siftDown(0);
    }

    @SuppressWarnings("unchecked")
    protected T getTip() {
        if (size == 0) {
            throw new IllegalStateException();
        }

        return (T) data[0];
    }

    protected float getTipKey() {
        if (size == 0) {
            throw new IllegalStateException();
        }

        return keys[0];
    }

    private void siftUp(int c) {
        for (int p = (c - 1) / 2; c != 0 && direction*keys[c] > direction*keys[p]; c = p, p = (c - 1) / 2) {
            Object pData = data[p];
            float pDist = keys[p];
            data[p] = data[c];
            keys[p] = keys[c];
            data[c] = pData;
            keys[c] = pDist;
        }
    }

    private void siftDown(int p) {
        for (int c = p * 2 + 1; c < size; p = c, c = p * 2 + 1) {
            if (c + 1 < size && direction*keys[c] < direction*keys[c + 1]) {
                c++;
            }
            if (direction*keys[p] < direction*keys[c]) {
                // Swap the points
                Object pData = data[p];
                float pDist = keys[p];
                data[p] = data[c];
                keys[p] = keys[c];
                data[c] = pData;
                keys[c] = pDist;
            } else {
                break;
            }
        }
    }

    public int size() {
        return size;
    }

    public int capacity() {
        return capacity;
    }

    public static final class Max<T> extends BinaryHeap<T> implements MaxHeap<T> {
        public Max() {
            super(defaultCapacity, 1);
        }
        public Max(int capacity) {
            super(capacity, 1);
        }
        public void removeMax() {
            removeTip();
        }
        public void replaceMax(float key, T value) {
            replaceTip(key, value);
        }
        public T getMax() {
            return getTip();
        }
        public float getMaxKey() {
            return getTipKey();
        }
    }
    public static final class Min<T> extends BinaryHeap<T> implements MinHeap<T> {
        public Min() {
            super(defaultCapacity, -1);
        }
        public Min(int capacity) {
            super(capacity, -1);
        }
        public void removeMin() {
            removeTip();
        }
        public void replaceMin(float key, T value) {
            replaceTip(key, value);
        }
        public T getMin() {
            return getTip();
        }
        public float getMinKey() {
            return getTipKey();
        }
    }
}