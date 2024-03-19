# MapLibre Tiles (MLT)

The MLT format is mainly inspired by the [Mapbox Vector Tile (MVT)](https://github.com/mapbox/vector-tile-spec) specification, but has been redesigned from the ground
to primarily improve on the following topics:
- **Improved compression ratio**: up to 6x on large tiles, based on a column oriented layout with (custom) lightweight encodings
- **Better decoding performance**: fast lightweight encodings which can be used in combination with SIMD/vectorization instructions
- **Support for linear referencing and m-values**: to efficiently support the upcoming next generation source formats such as Overture Maps (GeoParquet)
- **Support for a more complex type system**: Including nested properties, lists and maps
- **Support for 3D coordinates**
- **Improved processing performance**: Based on an in-memory format that can be processed efficiently on the CPU and GPU and loaded directly
  into GPU buffers partially (like polygons in WebGL) or completely (in case of WebGPU compute shader usage) without additional processing


## Comparison with Mapbox Vector Tiles

### Compression ratio
In the following, the sizes of MLT and MVT are compared on the basis of a selected set of representative tiles.

| Zoom level | Tile indices <br/>(minX,maxX,minY,maxY) | Tile Size Reduction (%) | 
|------------|-----------------------------------------|----------------------|
| 2          | 2,2,2,2                                 | 53                   |
| 3          | 4,4,5,5                                 | 48                   |
| 4          |                                         | 80                   |
| 5          | 16,17,20,21                             | 81                   |
| 6          | 32,34,41,42                             | 76                   |
| 7          | 66,68,83,85                             | 74                   |
| 8          | 132, 135, 170, 171                      | 74                   |
| 9          | 264, 266, 340, 342                      | 68                   |
| 10         | 530, 533, 682, 684                      | 60                   |
| 12         | 2130, 2134, 2733, 2734                  | 65                   | 
| 13         | 4264, 4267, 5467, 5468                  | 50                   |
| 14         | 8296, 8300, 10748, 10749                | 59                   |



### Decoding performance
Some initial tests comparing the decoding performance of the MLT with the MVT format for selected tiles in the browser, 
currently still without the use of SIMD instructions, show the following results:

| Zoom level | Decoding performance (MLT/MVT) |      
|------------|:------------------------------:|          
| 4          |              2.36              | 
| 5          |              2.74              |

