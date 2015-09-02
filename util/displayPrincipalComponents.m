% Displays the first fifteen principal components of a covariance matrix as individual images. 
% Input should be the result of readFloatOpenCVMatWritable.m
function [] = displayPrincipalComponents( openCVMatArray )
    [V, d]  = princomp( openCVMatArray );
    for n = 1:15
        column_data = V(:,n);
        I = mat2gray(vec2mat(column_data, 64));
        figure; imshow(I);
    end
end