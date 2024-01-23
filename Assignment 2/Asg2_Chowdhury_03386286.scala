// Databricks notebook source
""""
Module Name: CSC 735 HW2
Author : Shusmoy Chowdhury
Date of Creation: 10/7/2023
Purpose: Working with Sark with SQL and Dataframe way
"""
//importing necessary libraries
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions.asc
//loading  the dataset into databricks spark cluster
val df = spark.read.option("header", "true").option("inferSchema","true").csv("/FileStore/tables/movies.csv")
df.show(false)// showing the dataframe
df.count()// showing the number of elements availble in dataframe
df.createOrReplaceTempView("movies_table") // Creating table for the SQL

// COMMAND ----------

//Task 11
// SQL based spark code compute the number of movies produced in each year 
// Used Group By year to count movies for each year ignored the null year by adding condition year is not null
// Used order by to sort the output in ascending order
val sqlWay = spark.sql("""
SELECT year,count(title) as count
FROM movies_table where year is not null
GROUP BY year ORDER BY year
""")
sqlWay.show()// showing the output year and count(number of movies)

// COMMAND ----------

//task 12
// dataframe based spark code compute the number of movies produced in each year 
// Used Group By year to count movies for each year ignored the null year by adding condition year is not null
// Used sort function with asc to show the output in ascending order
val dataFrameWay =df.where("year is not null")
.groupBy("year")
.count()
.withColumnRenamed("count", "count")
.sort(asc("year"))
dataFrameWay.show()// showing the output year and count(number of movies)

// COMMAND ----------

//Task 13
// SQL based spark code find the five top most actors who acted in the most number of movies
// Used Group By actor to count movies by each actor
// Renamed the count(title) with number_of_movies
// Used Order by count(title) and DESC for sorting them in descending order based on the number of movies
// Used Limit 5 for showing the top 5 actors who acted in the most number of movies

val sqlWay2 = spark.sql("""
SELECT actor,count("title") as number_of_movies
FROM movies_table
GROUP BY actor ORDER BY count("title") DESC LIMIT 5
""")
sqlWay2.show()// showing the output actor and number_of_movies

// COMMAND ----------

//task 14
// Dataframe based spark code find the five top most actors who acted in the most number of movies
val dataFrameWay2 =df
.groupBy("actor")// Used Group By actor to count movies by each actor
.count()
.withColumnRenamed("count", "number_of_movies")// Renamed the count with number_of_movies
.sort(desc("number_of_movies"))// Used sort function with asc for sorting them in descending order based on the number of movies
.limit(5)// Used Limit 5 for showing the top 5 actors who acted in the most number of movies
dataFrameWay2.show()// showing the output actor and number_of_movies

// COMMAND ----------

//task 15
// Dataframe based spark code  to find the title and year for every movie that Tom Hanks acted in 
val dataFrameWay3=df
.where(col("actor").equalTo("Hanks, Tom")) // filtered actor column with actor equal "Hanks, Tom"
.select("year", "title").orderBy("year")// Select the year and title to show in output sorted by year

dataFrameWay3.show(false)// showing the out put year and title show(false) to show the full title
