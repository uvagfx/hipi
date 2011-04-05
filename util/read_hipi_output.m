function [ y ] = read_hipi_output( file )
    fid = fopen(file);
    if fid ~= 0
        fread(fid, 1, 'int32', 0, 'b');
        width = fread(fid, 1, 'int32', 0, 'b');
        height = fread(fid, 1, 'int32', 0, 'b');
        band = fread(fid, 1, 'int32', 0, 'b');
        y = reshape(fread(fid, width * height * band, 'float32', 0, 'b'), width, height, band);
    else
        y = [];
    end
end

