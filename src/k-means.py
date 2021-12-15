import os
import time
import boto3
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import argparse
import numpy as py
from math import radians, cos, sin, asin, sqrt, atan2, pi, degrees
from pyspark.sql.types import *
from shapely.geometry import Point
import geopandas as gpd
import matplotlib.pyplot as plt

geo_df_schema = StructType(
  [StructField('Latitude', DoubleType()),
   StructField('Longitude', DoubleType())]
)


def getHighestCenterDelta(c1, c2,func):
    """
        c1      -> cluster 1
        c2      -> cluster 2
        func    -> comparison function

        compares cluster 1 and cluster 2 using the distance comparision function specified as func
        returns the maxium distance between two corresponding points in the clusters
    """
    deltas = []
    
    for p1, p2 in zip(c1,c2):
        deltas.append(func(p1,p2))
    print(deltas)
    return max(deltas)

def assignPoint(point, centers,func):
    """
        point   -> point to be assigned a cluster
        centers -> list of lists of points represeting the current set of clusters
        func    -> distance comparison function

        returns the index of the cluster this point is closest to and the point
    """
    c_index = closestPoint(point, centers,func) # center that the point is closest to
    return c_index, point


def addPoints(p1, p2):
    """
        p1  -> point 1
        p2  -> point 2

        returns the midpoint between p1 and p2
    """

    lat1 = radians(p1[0])
    lon1 = radians(p2[1])
    
    # conver lat lon to xyz
    x = cos(lat1)*cos(lon1)
    y = cos(lat1)*sin(lon1)
    z = sin(lat1)
    

    lat2 = radians(p2[0])
    lon2 = radians(p2[1])
    
    x += cos(lat2)*cos(lon2)
    y += cos(lat2)*sin(lon2)
    z += sin(lat2)
    
    x = x/2
    y = y/2
    z = z/2
    
    # convert back to lat long
    r_lon = atan2(y,x)
    r_s = sqrt(x*x + y*y)
    r_lat = atan2(z, r_s)
    return degrees(r_lat), degrees(r_lon)

"""
def addPointToCluster(pointToAdd, pointsInCluster):
    lat = radians(pointToAdd[0])
    lon = radians(pointToAdd[1])
    
    x = cos(lat)*cos(lon)
    y = cos(lat)*sin(lon)
    z = sin(lat)
    
    for point in pointsInCluster:
        # convert lat long to x y 
        lat = radians(point[0])
        lon = radians(point[1])
        x += cos(lat)*cos(lon)
        y += cos(lat)*sin(lon)
        z += sin(lat)
        
        
    l = len(pointsInCluster) + 1
    x = x/l
    y = y/l
    z = z/l
    
    # convert back to lat long
    r_lon = atan2(y,x)
    r_s = sqrt(x*x + y*y)
    r_lat = atan2(z, r_s)
    return degrees(r_lat), degrees(r_lon)
"""

def closestPoint(point, clusters, func):
    """
        point       -> point to consider
        clusters    -> list points representing centroids
        func        -> comparison function

        returns the index of the cluster closest to the considered point
    """
    distances = []
    
    for cluster in clusters:
        d = func(point, cluster)
        distances.append(d)
    
    return distances.index(min(distances))

def GreatCircleDistance(pointA, pointB):
    """
        calculates the GCD between pointA and pointB
    """
    R = 6367 # km
    lat1 = radians(pointA[0])
    lat2 = radians(pointB[0])
    lon1 = radians(pointA[1])
    lon2 = radians(pointB[1])
    
    long_diff = radians(pointB[1] - pointA[1])
    lat_diff = radians(pointB[0] - pointA[0])
    
    a = sin(lat_diff/2)**2 + cos(lat1) * cos(lat2) * sin(long_diff/2)**2
    c = 2 * asin(min(1,sqrt(a)))
    d = R*c
    
    return d

def EuclideanDistance(pointA, pointB):
    """
        calculates the ED between pointA and pointB
    """
    R = 6367 # km
    lat1 = radians(pointA[0])
    lat2 = radians(pointB[0])
    lon1 = radians(pointA[1])
    lon2 = radians(pointB[1])
    
    # convert lat and long to cartesian points
    x1 = R*cos(lat1)*cos(lon1)
    y1 = R*cos(lat1)*sin(lon1)
    z1 = R*sin(lat1)
    
    x2 = R*cos(lat2)*cos(lon2)
    y2 = R*cos(lat2)*sin(lon2)
    z2 = R*sin(lat2)
    
    x = x2 - x1
    y = y2 - y1
    z = z2 - z1
    
    d = sqrt((x**2) + (y**2) + (z**2))
    
    return d

def initCentroids(k, func, points_rdd):
    """
        k           -> number of clusters
        func        -> comparision function
        points_rdd  -> rdd of potential points

        selects k centers from points_rdd using the method from the textbook
        we select the first point at random
        then we select the other points by attempting to ensure
        that they are as far from the other inital centroids as possible
    """
    clusters = []
    c1 = points_rdd.takeSample(False, 1)[0]
    clusters.append(c1)
    c = c1
    
    for i in range(k - 1):
        next_cluster = points_rdd.map(lambda point:(closestPoint(point, clusters, func), point))\
                                .map(lambda p: (func(clusters[p[0]], p[1]), p[1]))\
                                .max()[1]
        clusters.append(next_cluster)
    return clusters

def k_means(k, input_file, out_bucket, out_path, no_cache=False, distFunc=GreatCircleDistance):
    """
        k           -> number of clusters
        input_file  -> file of lat lon pairs
        out_bucket  -> s3 bucket to write to
        out_path    -> path to write at s3 bucket
        cache       -> persist rdds?
        distFunc    -> distance function

        does the main k-means login
    """
    config = SparkConf().setAppName(value="k-means").setMaster('local[*]')
    sc = SparkContext(conf=config)
    sql = SQLContext(sc)


    # rename columns
    geo_df = sql.read.csv(input_file,header=False).withColumnRenamed('_c0','Latitude').withColumnRenamed('_c1','Longitude')
    
    # generate rdd
    points_rdd = geo_df.select('Latitude', 'Longitude').rdd.map(lambda row: (float(row[0]), float(row[1])))
    if not no_cache:
        # persist if requested
        points_rdd.cache()

    # intialize the centroids
    centers = initCentroids(k,distFunc, points_rdd)

    conv_dist = 10          # dont stop until the deltas are less than 10km
    delta = conv_dist + 1   # ensure that while loop condition is met

    while (delta > conv_dist):
        # get new centers by assiging each point to a centroid,
        # find the cummulative midpoint between the points in each center
        # collect to a list of new_centers to replace the prior
        # set the delta to check convergence distance 
        new_centers = points_rdd.map(lambda x: assignPoint(x, centers,distFunc)).reduceByKey(addPoints).sortByKey().map(lambda x: x[1]).collect()
        delta = getHighestCenterDelta(centers, new_centers,distFunc)
        centers = new_centers

    # generate an RDD of the final assigned points based off their closest centroid
    assigned_points_rdd = points_rdd.map(lambda x: assignPoint(x,centers,distFunc))

    # convert the centers list to a df for plotting
    centers_df = assigned_points_rdd.reduceByKey(addPoints).sortByKey().map(lambda x: x[1]).toDF()
    
    # generate a df of (centroid, lat, lon)
    ap_df = assigned_points_rdd.map(lambda x: (x[0], x[1][0], x[1][1])).toDF()

    # convert to pandas for plotting
    centers_pandas_df = centers_df.toPandas()
    ap_pandas_df = ap_df.toPandas() 

    # generate geopandas dataframe
    centers_gdf = gpd.GeoDataFrame(centers_pandas_df, geometry=gpd.points_from_xy(centers_pandas_df._2,centers_pandas_df._1))
    assigned_points_gdf = gpd.GeoDataFrame(ap_pandas_df, geometry=gpd.points_from_xy(ap_pandas_df._3, ap_pandas_df._2))

    # set world background
    world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
    ax1 = world.plot()

    # plot assinged points, change the color by column _1 (cluster ID)
    ax2 = assigned_points_gdf.plot(ax=ax1,markersize=0.5,column='_1')
    centers_gdf.plot(ax=ax2, color='red')

    # save the file to disk so it can be uploaded to s3
    plot_file_name = os.path.basename(out_path)
    plt.savefig(plot_file_name)
    img_data = open(plot_file_name, 'rb')

    # upload the png to s3
    s3 = boto3.client('s3')
    s3.upload_file(plot_file_name, out_bucket, out_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='k-means')
    parser.add_argument('--nocache',  help='Persist RDDs',action='store_true', default=False)
    parser.add_argument('--distance',  help='Distance measure [euclidian|giant]',action='store', default='giant')
    parser.add_argument('input', help='Input file location', action='store')
    parser.add_argument('output_bucket', help='Name of bucket to write output', action='store')
    parser.add_argument('output_path', help='Path inside bucket to write output', action='store')
    parser.add_argument('k', help='how many means', action='store')


    args = parser.parse_args()

    func = GreatCircleDistance
    if args.distance == 'euclidian':
        func = EuclideanDistance

    # start timer
    start_time = time.time()

    # run main logic
    k_means(int(args.k), args.input, args.output_bucket, args.output_path, no_cache=args.nocache, distFunc=func)

    # end timer
    ellapsed = time.time() - start_time
    
    # attempt to write time and command line args to disk
    with open('runtimes.txt', 'a') as f:
        f.write(str(args) + ': \t' + str(ellapsed) + '\n')
