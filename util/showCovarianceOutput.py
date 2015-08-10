#!/usr/bin/python

import argparse, sys
import numpy as np
from matplotlib import pyplot as plt
import scipy.sparse.linalg as LA

# Parse command line
parser = argparse.ArgumentParser(description='Display the result of the covariance example.')
parser.add_argument('input', help="path to covariance result (whether mean image or covariance image)")
parser.add_argument("--opencv", help="covariance result is an OpenCVMatWritable object", action="store_true")
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
    type = header[1]
    rows = header[2]
    cols = header[3]
    mat = np.fromfile(f, dtype=np.dtype('f'))
    print mat.shape
    print mat
    
    #print ": ", width, ", height:", height, ", bands:", bands
    #print "storage format: ", storageformat
    #print "color space: ", colorspace
    #print "metadata length: ", metadatalength
    #print "metadata: ", metadata
    print "opencv type: ", type
    print "rows: ", rows, " cols: ", cols
    
    if (cols==psize):
        print "viewing mean image..."
        # Mean image, just display
        imgplt = plt.imshow(np.reshape(mat, (-1,psize)))
        imgplt.set_cmap('gray')
        imgplt.set_clim(0.0,1.0)
        plt.title('Average Patch')
        plt.colorbar()
        plt.show()
    else:
        print "viewing covariance image, computing eigenvectors..."
        # Covariance image, compute eigenvectors and display first 15 in 5x3 grid
        w, v = LA.eigs(np.reshape(mat, (cols,rows)), k=15)
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
        

