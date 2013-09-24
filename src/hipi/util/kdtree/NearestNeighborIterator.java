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

import hipi.util.kdtree.BinaryHeap;
import hipi.util.kdtree.IntervalHeap;
import hipi.util.kdtree.MinHeap;

import java.util.Arrays;
import java.util.Iterator;

/**
 *
 */
public class NearestNeighborIterator<T> implements Iterator<T>, Iterable<T> {
    private DistanceFunction distanceFunction;
    private float[] searchPoint;
    private MinHeap<KDNode<T>> pendingPaths;
    private IntervalHeap<T> evaluatedPoints;
    private int pointsRemaining;
    private float lastDistanceReturned;

    protected NearestNeighborIterator(KDNode<T> treeRoot, float[] searchPoint, int maxPointsReturned, DistanceFunction distanceFunction) {
        this.searchPoint = Arrays.copyOf(searchPoint, searchPoint.length);
        this.pointsRemaining = Math.min(maxPointsReturned, treeRoot.size());
        this.distanceFunction = distanceFunction;
        this.pendingPaths = new BinaryHeap.Min<KDNode<T>>();
        this.pendingPaths.offer(0, treeRoot);
        this.evaluatedPoints = new IntervalHeap<T>();
    }

    /* -------- INTERFACE IMPLEMENTATION -------- */

    @Override
    public boolean hasNext() {
        return pointsRemaining > 0;
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new IllegalStateException("NearestNeighborIterator has reached end!");
        }

        while (pendingPaths.size() > 0 && (evaluatedPoints.size() == 0 || (pendingPaths.getMinKey() < evaluatedPoints.getMinKey()))) {
            KDTree.nearestNeighborSearchStep(pendingPaths, evaluatedPoints, pointsRemaining, distanceFunction, searchPoint);
        }

        // Return the smallest distance point
        pointsRemaining--;
        lastDistanceReturned = evaluatedPoints.getMinKey();
        T value = evaluatedPoints.getMin();
        evaluatedPoints.removeMin();
        return value;
    }

    public float distance() {
        return lastDistanceReturned;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<T> iterator() {
        return this;
    }
}
