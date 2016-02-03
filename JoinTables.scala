import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.EmptyRDD
import scala.collection.mutable.ArrayBuffer
//import scala.collection.mutable.ListBuffer
//import util.control.Breaks._
import au.com.bytecode.opencsv.CSVParser
//import scala.util.control.Exception.allCatch
import scala.sys.process._
import java.text.DecimalFormat
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.broadcast._
import org.apache.spark.rdd.EmptyRDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
//import org.apache.spark.sql.functions.{col, min, max, mean, sum, count, avg}
import org.apache.spark.sql.types.{StructType,StructField,StringType};

object JoinApp {
 //Function for checking the Numeric data type of the variable 
  def isNumeric(s: String): Boolean = {
    (s.trim == "") || (s == "NA") || (allCatch opt s.toDouble).isDefined
  }
//dropheader function for dropping the header of the file 
  def dropHeader(data: RDD[String]): RDD[String] = {  
    data.mapPartitionsWithIndex((idx, lines) => {
      if (idx == 0) {
        lines.drop(1)
      }
      lines
    })
  }
  //Function for named Arguments 
  def processArgs(args:Array[String]):Map[String,String]={
    val namedArgsMap = args.map(arguementLine => arguementLine.split("--")).map(eachArgument =>(eachArgument(0),if(eachArgument.length==2) {eachArgument(1)} else {null})).toMap
   return namedArgsMap
  }
  
  def processArgs1(args:Array[String]):Map[String,String]={
    val namedArgsMap = args.map(arguementLine => arguementLine.split(":")).map(eachArgument =>(eachArgument(0),if(eachArgument.length==2) {eachArgument(1)} else {null})).toMap
   return namedArgsMap
  }

  def processArgs2(args:Array[String]):Map[String,Array[String]]={
    val namedArgsMap = args.map(arguementLine => arguementLine.split(":")).map(eachArgument =>(eachArgument(0),if(eachArgument.length==2) {eachArgument(1).split(',')} else {null})).toMap
   return namedArgsMap
  }

  def main(args: Array[String]) {
    val usage = """
    Usage: $SPARK_HOME/bin/spark-submit --class "JoinApp" --master <master host> target/scala-2.10/join-application_2.10-1.2.jar delimiter--<DELIMITER(in quotes and separated by space)> inputFile--<inputHDFSPath for 2 files separated by comma > joinType--<join type between different files> keys--"Left:<enter the keys of first file >Right:<enter the keys of second file>#Left:<>Right:<>" select--"t1: #t3: #t4:" output--<outputPath>
  """
  //Check for the no. of arguments 
   if (args.length != 5)
    {
        println(usage)
        sys.exit(1)
    }
    val conff = new SparkConf().setAppName("Group-By Application").set("spark.shuffle.consolidateFiles", "true")
    val conf=conff.set("spark.sql.retainGroupColumns","false")
    val sc = new SparkContext(conf)
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val startTime=System.nanoTime
      val namedArgsMap=processArgs(args)
    //Check for the existence of input file and its delimiter
   if ((namedArgsMap.isEmpty)|(namedArgsMap.size!=5)|(namedArgsMap.exists(_._1==null))|(namedArgsMap.exists(_._2==null))|(namedArgsMap.exists(_._1==""))|(namedArgsMap.exists(_._2==""))){ 
        println(usage + "wrong Input")
        sys.exit(1)}  
    var delimiter,filePath,jType,leftRightKey,selectVar,outputPath=""
    namedArgsMap.keys.foreach(i => i match {case "delimiter" => delimiter = namedArgsMap(i) 
        case "inputFile" => filePath = namedArgsMap(i) 
        case "joinType" => jType = namedArgsMap(i)
        case "keys" => leftRightKey = namedArgsMap(i)
       case "select" => selectVar =namedArgsMap(i)
        case "output" => outputPath = namedArgsMap(i)
        case others => println("Incorrect argument: " + others + usage)
                  sys.exit(2)
        })

    val delimiterArr = delimiter.split(" ")
    var splitter : ArrayBuffer[Char] = ArrayBuffer()
    for(i<- 0 until delimiterArr.length)
    {
      delimiterArr(i) match {
        case "\\t" => splitter += '\t'
        case x => splitter += x.head  
      }
    }
    
   // val numberFile= filePath.length
    val filePathArr= filePath.split(',')
    if(filePathArr.length != 2)
    {
      println("Number of files are :" + filePathArr.length + " please enter two files")
      sys.exit(8)
    }
    var file : ArrayBuffer[RDD[String]]= ArrayBuffer()
    for(i<-0 until filePathArr.length)
    {
      file+= sc.textFile(filePathArr(i))
    }

    if(splitter.length != filePathArr.length)
    {
      println("Number of delimiters are not equivalent to number of input files" + "no. of files are :" + filePathArr.length +"no of splitter are :" + splitter.length)
      sys.exit(3)
    }

    val keyArray= leftRightKey.split('#')
     val jTypeArr = jType.split(",")
    val numJoin =jTypeArr.length
    if(numJoin != keyArray.length)
    {
      println("Set of Keys entered for join operation are not equal to the number of join , number of joins are : " + numJoin +"set of keys are :" + keyArray.length)
      sys.exit(4)
    }

  var joinRDD = file(0)
  val left_splitter = splitter(0)
   for(i<-0 until numJoin)
    {
      val keyArraySplit= keyArray(i).split(' ')
      val keysArgsMap = processArgs1(keyArraySplit)
      var leftkeys, rightkeys =""
        keysArgsMap.keys.foreach(i => i match {case "Left" => leftkeys = keysArgsMap(i)
          case "Right" => rightkeys = keysArgsMap(i)
         // case "select "
          case others => println("Incorrect Arguments in Keys : "+ others + usage)
          sys.exit(5)
          })

      val leftkeysArr = leftkeys.split(',')
      val rightkeysArr = rightkeys.split(',')
      if(leftkeysArr.length != rightkeysArr.length)
      {
        println("Number of keys for left file are :" + leftkeysArr.length + " Number of keys for right file are :" +rightkeysArr.length + " Keys for both files should be same Please enter equal number of keys ")
        sys.exit(9)
      }

      val choice = jTypeArr(i)
      val left_file = joinRDD
      val right_file = file(i+1)
      val withoutHeader_left= dropHeader(left_file)
      val withoutHeader_right = dropHeader(right_file)
      val table1 = withoutHeader_left
      val schemaString1= left_file.take(1).flatMap(s => s.split(left_splitter)).map(_.replaceAll("\"",""))
      val schema1 = StructType(schemaString1.map(fieldName => StructField(fieldName, StringType, true))) 
      val rowRDD1 = table1.map(p=>{
        Row.fromSeq(p.split(left_splitter.toString,-1))
      })
      val myDF1 = sqlContext.applySchema(rowRDD1, schema1)
      val table2 = withoutHeader_right
      val schemaString2= right_file.take(1).flatMap(s => s.split(splitter(i+1))).map(_.replaceAll("\"",""))
      val schema2 = StructType(schemaString2.map(fieldName => StructField(fieldName, StringType, true))) 
      val rowRDD2 = table2.map(p=>{
        Row.fromSeq(p.split(splitter(i+1).toString,-1)) 
        })
      val myDF2 = sqlContext.applySchema(rowRDD2, schema2) 
      val joinExpr = leftkeysArr.zip(rightkeysArr).map{case(c1,c2)=> myDF1(c1)=== myDF2(c2)}.reduce(_&&_)
      val joinDF = myDF1.join(myDF2, joinExpr, choice)
      //joinDF.printSchema
      val joinRDD_withoutHeader = joinDF.rdd.map{case(x)=>x.toString.stripSuffix("]").stripPrefix("[")}
      val header = (schemaString1.mkString(",") + "," + schemaString2.mkString(","))
      val headerRDD: RDD[String] = sc.parallelize(Array(header))
      joinRDD = headerRDD.union(joinRDD_withoutHeader)
    } 

      val selectVarArr= selectVar.split('#')
      val selectMap= processArgs2(selectVarArr)
      val length= file.length

      val withoutHeader_joinRDD = dropHeader(joinRDD)
      val table = withoutHeader_joinRDD
      val schemaString= joinRDD.take(1).flatMap(s => s.split(',')).map(_.replaceAll("\"",""))
      val schema = StructType(schemaString.map(fieldName => StructField(fieldName, StringType, true))) 
      val rowRDD = table.map(p=>{
        Row.fromSeq(p.split(','.toString,-1))
      })
      val joinDF_final= sqlContext.applySchema(rowRDD, schema)
      // val outputDF = joinDF_final.select(selectVarArr.head, selectVarArr.tail: _*) 

      val outputRDD = outputDF.rdd.map{case(x)=>x.toString.stripSuffix("]").stripPrefix("[")}
      val date = new Date();
      val formatter = new SimpleDateFormat("MMddyyyy_hhmmss")
      val formattedDate = formatter.format(date)
      val output_hdfs = outputPath + "_" + formattedDate
      outputRDD.saveAsTextFile(output_hdfs)
      println("SUCCESS")
      sys.exit(0)
  }
}
