% Converts an OpenCVMatWritable which encapsulates a Mat of floats into a MATLAB matrix
function [ y ] = readFloatOpenCVMatWritable( filePath )
    fid = fopen( filePath );
    if fid ~= 0
        type = fread(fid, 1, 'int32', 0, 'b');
        rows = fread(fid, 1, 'int32', 0, 'b');
        cols = fread(fid, 1, 'int32', 0, 'b');
        y = reshape(fread(fid, 'float32', 0, 'b'), cols, rows);
    else
        y = [];
    end
end