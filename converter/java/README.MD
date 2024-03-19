

Generate Java metadata classes from ``maplibre_tile_metdata.proto`` file:  
```console
protoc --java_out=src/main/java -I "..\..\spec\schema" maplibre_tile_metadata.proto
```
