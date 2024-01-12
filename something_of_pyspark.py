pandas
pyspark


1. reading data from file  (.xlsx, .csv, .parquet, delta tables)
2. writing data to a file
3. find columns(headers) names
4. change headers names
5. get first n records 
6. replacing some data in a colum
7. removing leading 0s
8. changing datatype
9. joining
10. group by
11. finding sum of two or more columns
12. concateing two columns
13. Filters




reading data  from file:
Paruqet:  spark.read.format("parquet").load(path)
Csv: spark.read.format("csv").load(path)
Xlsx: spark.read.format("com.crealytics.spark.excel").load(filePath)
delta table:  spark.sql("sql select query")

find columns(headers) names
dataframe.columns

change headers names
newCols = [new header list]
M1: df = df.toDF(*newCols)

#### replace spaces with undersocre in header names
NewColumns=(column.replace(' ', '_') for column in sdf.columns)
sdf = sdf.toDF(*NewColumns)

##### replace % in TP_Contribution column with '#'
consolodated_df = consolodated_df.withColumn('TP_Contribution', F.regexp_replace(F.col('TP_Contribution'), r'%', '#'))
consolodated_df.display()

####Filter for YEar 2023--- filter()
df = df.filter( (df.Year==2023) & (df.Quarter=='Q2') )
df.display()

#### withColumn()
### add a new column
sdf = sdf.withColumn('Age',lit(''))
# sdf.display()

#### withColumn()  when()
#### add a column result. Assign pass to ece branch and fail to other branches
sdf = sdf.withColumn('Result', when(col('STUDENT_BRANCH')=='ECE','PASS').otherwise('FAIL') )

sdf = sdf.withColumn('Age', when(col('STUDENT_NAMES')=='Uma','20').otherwise('60') )
sdf.display()