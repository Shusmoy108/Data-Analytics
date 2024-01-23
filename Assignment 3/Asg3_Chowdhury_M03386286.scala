// Databricks notebook source
""""
Module Name: CSC 735 HW3
Author : Shusmoy Chowdhury
Date of Creation: 10/15/2023
Purpose: Working with Sark with Join and Window
"""
//importing necessary libraries
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.asc
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
//loading  the dataset into databricks spark cluster
val df1 = spark.read.option("header", "true") 
 .option("inferSchema","true")
 .csv("/FileStore/tables/movies.csv")

val df2 = spark.read.option("header", "true") 
 .option("inferSchema","true")
.csv("/FileStore/tables/movie_ratings.csv")
// Creating table for the SQL
df1.createOrReplaceTempView("movies_table") 
df2.createOrReplaceTempView("movie_reviews_table")
//df2.show(false)

// COMMAND ----------

//task3
// Write DataFrame-based Spark code to find the number of distinct movies in the file movies.csv
df1.select("title").distinct().count() //selecting the distinct movie title and counting them

// COMMAND ----------

//task4
//Write DataFrame-based Spark code to find the titles of the movies that appear in the file movies.csv but do not have a rating in the file movie_ratings.csv. 
val df2join = df2.withColumnRenamed("title", "join_title")// renaming the title in movie_rating dataframe
df1.join(df2join, 'title==='join_title,"left_anti" ).select("title").show(false) // left anti join on title column  with movie and move_rating dataframe. showing the title as output  

// COMMAND ----------

//task5
//Write DataFrame-based Spark code to find the number of movies that appear in the ratings file (i.e., movie_ratings.csv) but not in the movies file (i.e., movies.csv).
val df1join = df1.withColumnRenamed("title", "join_title")// renaming the title of the movie dataframe for join
df2.join(df1join, 'title==='join_title, "left_anti").select("title").distinct().count()// left anti join on title column  with move_rating and movie dataframe. showing the distinct title count as output  

// COMMAND ----------

//task6
//Write DataFrame-based Spark code to find the total number of distinct movies that appear in either movies.csv, or movie_ratings.csv, or both.
df1.join(df2,Seq("title"),"outer").select("title").distinct().count() // Outer joiining the two dataframe based on similar title and showinng the distinct title count as output 

// COMMAND ----------

//task7
//Write DataFrame-based Spark code to find the title and year for movies that were remade. These movies appear more than once in the ratings file with the same title but different years. Sort the output by title.
val df2join=df2.groupBy("title").count().where("count>1").withColumnRenamed("title","join_title")// Grouping the moview _rating dataframe with title and counting them. Filtering where count>1 to get the remake, renaming the column title of the movie_rating dataframe for join
df2.join(df2join,'title==='join_title).select("title","year").sort(asc("title"))show(false)// inner join with movew _rating dataframe , selecting year and title showing the output sorted ascending order with title

// COMMAND ----------

//task8
//Write DataFrame-based Spark code to find the rating for every movie that the actor "Branson, Richard" appeared in. Schema of the output should be (title, year, rating
val df1join = df1.withColumnRenamed("title", "join_title").withColumnRenamed("year", "join_year").select("join_title","actor", "join_year") //renaming the column title,year of the movie_rating dataframe for join
df2.join(df1join,'title==='join_title) // joining the dataframes based on similar title
.where(col("actor").equalTo("Branson, Richard"))// filtering actor equal "Branson, Richard"
.where(df1join.col("join_year")===df2.col("year"))// filtering the remake actor did not worked
.select("title","year","rating").show(false)// selecting title, year, rating as output

// COMMAND ----------

//task9
// Write DataFrame-based Spark code to find the highest-rated movie per year and include all the actors in that movie. The output should have only one movie per year, and it should contain four columns: year, movie title, rating, and a list of actor names. Sort the output by year.
val windowSpec = Window.partitionBy("year").orderBy(col("rating").desc)// creating window partition by year and descending order by rating to get the top rated movies
val rankedmovie=df2.withColumn("rank", rank().over(windowSpec)).where('rank === 1) // using rank function and filtering based on rank =1 to get the top movie of each year
rankedmovie.join(df1,Seq("title","year"),"left_outer")// left outer join two dataframes based on year and title
.drop(df1.col("year"))// dropping duplicate column year
.groupBy("year","title","rating")// grouping the dataframe based on "year","title","rating"
.agg(collect_list("actor").as("actor_list"))// aggreating the actors as a list and named the column as actor_list
.select("year","title","rating","actor_list").show(false)// selecting showing the "year","title","rating","actor_list" as output

// COMMAND ----------

//task10
//Write DataFrame-based Spark code to determine which pair of actors worked together most. Working together is defined as appearing in the same movie. The output should have three columns: actor 1, actor 2, and count. The output should be sorted by the count in descending order.
val windowSpec2 = Window.orderBy(col("actor").asc)// creating window ascending order by rating to get the top rated movies
val rankedmovie=df1.withColumn("rank", rank().over(windowSpec2)).withColumnRenamed("actor", "actor 1")// using rank function to rank all the actor and renaming the actor column as actor 1
val newrankedmovie= rankedmovie.withColumnRenamed("rank","rank_new").withColumnRenamed("title","new_title").withColumnRenamed("actor 1","actor 2").withColumnRenamed("year","year_new") // creating another dataframe with renaming rank as rank_new, title as new_title, actor 1 as actor 2 and year as year_new
val n=rankedmovie.join(newrankedmovie).where(rankedmovie("title")===newrankedmovie("new_title")).where(rankedmovie("rank")>newrankedmovie("rank_new"))//joining the two datafreme based on similar title and actor 1 rank greater than actor 2 rank to avoid similar actor. 
.groupBy("actor 1","actor 2")//Group by the dataframw with actor 1 and actor 2
n.count().sort(desc("count")).show()// counting the movies based on actor pair and showing the output as descending order
