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
 *
 */
class KDNode<T> {
    // All types
    protected int dimensions;
    protected int bucketCapacity;
    protected int size;

    // Leaf only
    protected float[][] points;
    protected Object[] data;

    // Stem only
    protected KDNode<T> left, right;
    protected int splitDimension;
    protected float splitValue;

    // Bounds
    protected float[] minBound, maxBound;
    protected boolean singlePoint;

    protected KDNode(int dimensions, int bucketCapacity) {
        // Init base
        this.dimensions = dimensions;
        this.bucketCapacity = bucketCapacity;
        this.size = 0;
        this.singlePoint = true;

        // Init leaf elements
        this.points = new float[bucketCapacity+1][];
        this.data = new Object[bucketCapacity+1];
    }

    /* -------- SIMPLE GETTERS -------- */

    public int size() {
        return size;
    }

    public boolean isLeaf() {
        return points != null;
    }

    /* -------- OPERATIONS -------- */

    public void addPoint(float[] point, T value) {
    	KDNode<T> cursor = this;
        while (!cursor.isLeaf()) {
            cursor.extendBounds(point);
            cursor.size++;
            if (point[cursor.splitDimension] > cursor.splitValue) {
                cursor = cursor.right;
            }
            else {
                cursor = cursor.left;
            }
        }
        cursor.addLeafPoint(point, value);
    }

    /* -------- INTERNAL OPERATIONS -------- */

    public void addLeafPoint(float[] point, T value) {
        // Add the data point
        points[size] = point;
        data[size] = value;
        extendBounds(point);
        size++;

        if (size == points.length - 1) {
            // If the node is getting too large
            if (calculateSplit()) {
                // If the node successfully had it's split value calculated, split node
                splitLeafNode();
            } else {
                // If the node could not be split, enlarge node
                increaseLeafCapacity();
            }
        }
    }

    private boolean checkBounds(float[] point) {
        for (int i = 0; i < dimensions; i++) {
            if (point[i] > maxBound[i]) return false;
            if (point[i] < minBound[i]) return false;
        }
        return true;
    }

    private void extendBounds(float[] point) {
        if (minBound == null) {
            minBound = Arrays.copyOf(point, dimensions);
            maxBound = Arrays.copyOf(point, dimensions);
            return;
        }

        for (int i = 0; i < dimensions; i++) {
            if (Float.isNaN(point[i])) {
                if (!Float.isNaN(minBound[i]) || !Float.isNaN(maxBound[i])) {
                    singlePoint = false;
                }
                minBound[i] = Float.NaN;
                maxBound[i] = Float.NaN;
            }
            else if (minBound[i] > point[i]) {
                minBound[i] = point[i];
                singlePoint = false;
            }
            else if (maxBound[i] < point[i]) {
                maxBound[i] = point[i];
                singlePoint = false;
            }
        }
    }

    private void increaseLeafCapacity() {
        points = Arrays.copyOf(points, points.length*2);
        data = Arrays.copyOf(data, data.length*2);
    }

    private boolean calculateSplit() {
        if (singlePoint) return false;

        float width = 0;
        for (int i = 0; i < dimensions; i++) {
            float dwidth = (maxBound[i] - minBound[i]);
            if (Float.isNaN(dwidth)) dwidth = 0;
            if (dwidth > width) {
                splitDimension = i;
                width = dwidth;
            }
        }

        if (width == 0) {
            return false;
        }

        // Start the split in the middle of the variance
        splitValue = (minBound[splitDimension] + maxBound[splitDimension]) * 0.5f;

        // Never split on infinity or NaN
        if (splitValue == Float.POSITIVE_INFINITY) {
            splitValue = Float.MAX_VALUE;
        }
        else if (splitValue == Float.NEGATIVE_INFINITY) {
            splitValue = -Float.MAX_VALUE;
        }

        // Don't let the split value be the same as the upper value as
        // can happen due to rounding errors!
        if (splitValue == maxBound[splitDimension]) {
            splitValue = minBound[splitDimension];
        }

        // Success
        return true;
    }

    private void splitLeafNode() {
        right = new KDNode<T>(dimensions, bucketCapacity);
        left = new KDNode<T>(dimensions, bucketCapacity);

        // Move locations into children
        for (int i = 0; i < size; i++) {
            float[] oldLocation = points[i];
            Object oldData = data[i];
            if (oldLocation[splitDimension] > splitValue) {
                right.addLeafPoint(oldLocation, (T) oldData);
            }
            else {
                left.addLeafPoint(oldLocation, (T) oldData);
            }
        }

        points = null;
        data = null;
    }
}
