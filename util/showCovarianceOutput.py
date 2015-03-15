#!/usr/bin/python

import argparse, sys
import numpy as np
from matplotlib import pyplot as plt
import scipy.sparse.linalg as LA

# Parse command line
parser = argparse.ArgumentParser()
parser.add_argument('input')
args = parser.parse_args()

# Get input file
fname = args.input
print "Input file:", fname

# Set patch size
psize = 48

f = open(fname,"rb")
try:
    header = np.fromfile(f, dtype=np.dtype('>i4'), count=4)
    print header
    width = header[1]
    height = header[2]
    bands = header[3]
    mat = np.fromfile(f, dtype=np.dtype('>f4'))
    print "dim:", width, ":", height, ":", bands
    if (width==psize):
        # Mean image, just display
        imgplt = plt.imshow(mat.reshape((psize,psize)))
        imgplt.set_cmap('gray')
        imgplt.set_clim(0.0,1.0)
        plt.title('Average Patch')
        plt.colorbar()
        plt.show()
    else:
        # Covariance image, compute eigenvectors and display first 15 in 5x3 grid
        w, v = LA.eigs(mat.reshape((width,height)), k=15)
        img = np.zeros((psize*3,psize*5))
        for j in range(0,3):
            for i in range(0,5):
                for y in range(0,psize):
                    for x in range(0,psize):
                        img[(j*psize+y),(i*psize+x)] = v[:,j*5+i].reshape(psize,psize)[y,x]
        imgplt = plt.imshow(np.real(img))
        imgplt.set_cmap('gray')
        imgplt.set_clim(-0.1,0.1) # Guess range
        plt.title('Principal Components of Covariance Matrix')
        plt.colorbar()
        plt.show()
finally:
    f.close()

