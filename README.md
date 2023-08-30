# Image_Smoothing_MapReduce
A simple image smoothing code by MapReduce.

First, for each pixel, its x and y values are taken as the key, and its RGB value is taken as the value and sent to the mapper. 
Then, for each x, y pair, nine RGB values (as the RGB values of surrounding pixels) need to be sent to the reducer.

Then, in the reducer, an averaging operation is performed, and the resulting RGB value is considered as the RGB value for the desired pixel. 
In this context, an image has been sent to the MapReduce code using a file where, for each pixel, its RGB value is stored in hexadecimal format. 
An example of such an input file has been provided.
