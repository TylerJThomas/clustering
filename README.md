Help message:

```
usage: k-means.py [-h] [--nocache] [--distance DISTANCE] [--scale SCALE]
                  input output_bucket output_path k

k-means

positional arguments:
  input                Input file location
  output_bucket        Name of bucket to write output
  output_path          Path inside bucket to write output
  k                    how many means

optional arguments:
  -h, --help           show this help message and exit
  --nocache            Persist RDDs
  --distance DISTANCE  Distance measure [euclidian|giant]
  --scale SCALE        Scale to plot on [world|usa]
```

example use:

```
spark-submit k-means.py s3://cleaned-geo-final/clean_devicestatus.txt geo-final small/5/euclidian/nocache.png 5 \
			--scale usa --distance euclidian --nocache
```
