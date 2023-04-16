>>val ratingsRDD=sc.textFile("spark/ratings.dat")

# map the RDD of strings to an RDD of integers that contain the movie IDs.
>>val movies=ratingsRDD.map(line=line.split("::")(1).toInt)

# create an RDD of key-value pairs where each movie ID is paired with a value of 1.
val movies_pair=movies.map(mv=(mv,1))

#reduce the RDD by key (movie ID) and adds up the values to get the total count of ratings for each movie.
>>val movies_count=movies_pair.reduceByKey((x,y)=x+y)

# sort the RDD by the count of ratings in descending order.
>>val movies_sorted=movies_count.sortBy(x=x._2,false,1)

#  take the top 10 movies with the most ratings and convert the result to a list.
>>mv_top10List=movies_sorted.take(10).toList

# parallelize the list of top 10 movies as an RDD.
>>val mv_top10RDD=sc.parallelize(mv_top10List)

#read in the movies dataset as an RDD of strings and map it to an RDD of key-value pairs where each movie ID is paired with its name.
>>val mv_names=sc.textFile(....Movielensmovies.dat).map(line=(line.split()(0).toInt,line.split()(1)))

#join the movies dataset RDD with the RDD of top 10 movies to get the names of the top 10 movies.
>>val join_out=mv_names.join(mv_top10RDD)

# sort the resulting RDD by the count of ratings in descending order, maps each entry to a string in the format "movie ID,movie name,count of ratings", repartitions the RDD to a single partition, and saves the output as a text file named "Top-10-CSV".
>>join_out.sortBy(x=x._2._2,false).map(x= x._1+","+x._2._1+","+x._2._2).repartition(1).saveAsTextFile("Top-10-CSV")
