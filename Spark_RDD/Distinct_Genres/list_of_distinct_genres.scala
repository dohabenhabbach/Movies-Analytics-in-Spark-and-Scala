val movies_rdd=sc.textFile("../../Movielens/movies.dat")
val genres=movies_rdd.map(lines=>lines.split("::")(2))
val testing=genres.flatMap(line=>line.split('|'))
val genres_distinct_sorted=testing.distinct().sortBy(_(0))
genres_distinct_sorted.saveAsTextFile("result")