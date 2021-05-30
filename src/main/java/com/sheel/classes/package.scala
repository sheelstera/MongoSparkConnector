package com.sheel

import com.mongodb.client.MongoCollection
import com.mongodb.spark.{MongoConnector, MongoSpark}
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import com.sheel.classes.accumulator.CustomAccumulator
import org.bson.Document

import java.util
import scala.collection.mutable.Map

/**
 * @author Sheel Pancholi
 * @project Custom Mongo Spark Connector
 * @created 14 May, 2021
 */
package object classes {

  import org.apache.spark.sql.{DataFrame, SparkSession}
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types._
  import scala.annotation.tailrec

  val sparkSession=SparkSession.builder().getOrCreate()
  implicit class DFHelpers(df: DataFrame) {
    def columns = {
      val dfColumns = df.columns.map(_.toLowerCase)
      df.schema.fields.flatMap { data =>
        data match {
          case column if column.dataType.isInstanceOf[StructType] => {
            column.dataType.asInstanceOf[StructType].fields.map { field =>
              val columnName = column.name
              val fieldName = field.name
              col(s"${columnName}.${fieldName}").as(s"${columnName}___${fieldName}")
            }.toList
          }
          case column => List(col(s"${column.name}"))
        }
      }
    }

    def flatten: DataFrame = {
      val empty = df.schema.filter(_.dataType.isInstanceOf[StructType]).isEmpty
      empty match {
        case false => df.select(columns: _*).flatten
        case _ => df
      }
    }
    def explodeColumnsExcludingArray = df.flatten

    def explodeColumnsIncludingArray = {
      @tailrec
      def columns(cdf: DataFrame):DataFrame = cdf.schema.fields.filter(_.dataType.typeName == "array") match {
        case c if !c.isEmpty => columns(c.foldLeft(cdf)((dfa,field) => {
          dfa.withColumn(field.name,explode_outer(col(s"${field.name}"))).flatten
        }))
        case _ => cdf
      }
      columns(df.flatten)
    }
  }
  implicit class MongoStreamWriter(batchDF:DataFrame) {
    def processBatch={
      batchDF.cache()

      def removeNullFields(map:scala.collection.mutable.Map[String,Any])={
        map.keySet
          .foreach(
            key=>{
              map.get(key) match {
                case Some(null) => {
                  map.remove(key)
                }
                case _ => _
              }

            }
          )
      }

      def traverseOrCreatePaths(pathToTraverseForThisField:Array[String],fromDocument:Document)={
        import scala.collection.JavaConverters._
        var innerDoc = fromDocument
        val javaList = pathToTraverseForThisField.toList.asJava
        val array= new Array[String](javaList.size)
        javaList.toArray(array)
        val fieldsToTraverse = java.util.Arrays.copyOf(array,array.length-1)
        fieldsToTraverse
          .foreach(
            field =>{
              val resultObject = innerDoc.get(field)
              if(resultObject!=null){
                innerDoc = resultObject.asInstanceOf[Document]
              }
              else{
                var innerDoc2=new Document()
                innerDoc.append(field,innerDoc2)
                innerDoc=innerDoc2
              }
            }
          )
        innerDoc
      }

      def convertToSimpleDocument(list:List[scala.collection.mutable.Map[String,Any]])={
        val newList: util.ArrayList[Object]=new util.ArrayList()
        list
          .foreach(
            e=>{
              newList.add(e.head._2.asInstanceOf[Object])
            }
          )
        newList
      }

      def convertToComplexDocument(list:List[scala.collection.mutable.Map[String,Any]])={
        def getFlattenedFieldPathsRelativeToArrayElementKey(inputMap:scala.collection.mutable.Map[String,Any])=inputMap.keySet.toArray

        var arrayItemDocument=new Document()
        val newList= new util.ArrayList[Document]()
        list
          .foreach(
            element=>{
              removeNullFields(element)
              arrayItemDocument=new Document()
              val fields=getFlattenedFieldPathsRelativeToArrayElementKey(element)
              fields
                .foreach(
                  field =>{
                    val pathToTraverseForThisField = field.split("___")
                    if(pathToTraverseForThisField.length>1){
                      var leafDocumentWithArrayField=traverseOrCreatePaths(pathToTraverseForThisField,arrayItemDocument)
                      leafDocumentWithArrayField.put(pathToTraverseForThisField(pathToTraverseForThisField.length-1),element.get(field).get)
                    }
                    else {
                      arrayItemDocument.put(pathToTraverseForThisField(0),element.get(field).get)
                    }
                  }
                )
              newList.add(arrayItemDocument)
            }
          )
        newList
      }

      def updateDocumentForNonArrayFields(document:Document,maps:Array[scala.collection.mutable.Map[String,Any]])={
        maps
          .foreach(
            map=>{
              removeNullFields(map)
              if(map.get("_id").get.asInstanceOf[String]==document.get("_id").toString){
                val fieldsToUpdate=
                  map.keySet
                    .filter(!_.equals("_id"))
                fieldsToUpdate
                  .foreach(
                    fieldToUpdate=>{
                      val value=map.get(fieldToUpdate).get
                      val fieldOnEachLevel=fieldToUpdate.split("___")
                      if(fieldOnEachLevel.length>1){
                        var innerDoc=traverseOrCreatePaths(fieldOnEachLevel,document)
                        innerDoc.put(fieldOnEachLevel(fieldOnEachLevel.length-1),value)
                      }
                      else{
                        document.put(fieldOnEachLevel(0),value)
                      }
                    }
                  )
              }
            }
          )
      }

      def updateDocumentForArrayFields(document:Document,map:scala.collection.mutable.Map[(String,Boolean,String,String,String),List[Map[String,Any]]])={
        map
          .foreach(
            element=>{
              if(element._1._4 == document.get("_id").toString){
                val fieldOnEachLevel=element._1._1.split("___")
                if(fieldOnEachLevel.length>1){
                  var innerDoc = traverseOrCreatePaths(fieldOnEachLevel,document)
                  if(element._1._2||innerDoc.get(fieldOnEachLevel(fieldOnEachLevel.length-1))==null){
                    if(element._1._5 == "complex"){
                      innerDoc.put(fieldOnEachLevel(fieldOnEachLevel.length-1),convertToComplexDocument(element._2))
                    }else{
                      innerDoc.put(fieldOnEachLevel(fieldOnEachLevel.length-1),convertToSimpleDocument(element._2))
                    }
                  }
                }else{
                  if(element._1._2||document.get(fieldOnEachLevel(fieldOnEachLevel.length-1))==null){
                    if(element._1._5 == "complex"){
                      document.put(fieldOnEachLevel(0),convertToComplexDocument(element._2))
                    }else{
                      document.put(fieldOnEachLevel(0),convertToSimpleDocument(element._2))
                    }
                  }
                }
              }
            }
          )
      }

      var map:scala.collection.mutable.Map[(String,Boolean,String,String,String),List[scala.collection.mutable.Map[String,Any]]]=scala.collection.mutable.Map.empty
      var decisionMap:scala.collection.mutable.Map[String,(String,Boolean,String)]=scala.collection.mutable.Map.empty
      val explodedDF0=batchDF.explodeColumnsExcludingArray
      val nonArrayFields=explodedDF0.schema.fields
        .filter(!_.dataType.typeName.equals("array"))
        .map(_.name)
      val arrayFields=explodedDF0.schema.fields
        .filter(_.dataType.typeName.equals("array"))
        .map(_.name)
      arrayFields
        .foreach(
          arrayField=>{
            decisionMap+=(arrayField->(arrayField,true,""))
          }
        )
      val mc = new CustomAccumulator
      sparkSession.sparkContext.register(mc)
      arrayFields
        .foreach(
          arrayField =>{
            var listOfDocs:List[scala.collection.mutable.Map[String,Any]]=List.empty
            val explodedArrayFieldDF=
              explodedDF0
                .select(col("_id"),col(s"$arrayField"))
                .filter(s"$arrayField is not null")
                .explodeColumnsIncludingArray
            val columnKeyList=explodedArrayFieldDF.columns
            explodedArrayFieldDF.rdd.repartition(1)
              .foreach(e=>{
                val mapVersionOfRow=e.getValuesMap(Seq(columnKeyList:_*))
                var normalizedMap:scala.collection.mutable.Map[String,Any]=scala.collection.mutable.Map.empty
                if(
                  mapVersionOfRow
                    .filter(p=>{!(p._1=="_id")}).size==1
                ){
                  mapVersionOfRow
                    .foreach(
                      element=>{
                        if(!element._1.equals("_id")){
                          normalizedMap+=(element._1 -> element._2)
                        }
                      }
                    )
                  listOfDocs = normalizedMap :: listOfDocs
                  mc.add(
                    (
                      (
                      decisionMap(arrayField)._1,
                      decisionMap(arrayField)._2,
                      decisionMap(arrayField)._3,
                      mapVersionOfRow.get("_id").get.asInstanceOf[String],
                      "primitive"
                    ),listOfDocs
                    )
                  )
                }
                else{
                  mapVersionOfRow
                    .foreach(
                      element=>{
                        if(!element._1.equals("_id")){
                          normalizedMap+=(element._1.substring(arrayField.length+3) -> element._2)
                        }
                      }
                    )
                  listOfDocs = normalizedMap :: listOfDocs
                  mc.add(
                    (
                      (
                        decisionMap(arrayField)._1,
                        decisionMap(arrayField)._2,
                        decisionMap(arrayField)._3,
                        mapVersionOfRow.get("_id").get.asInstanceOf[String],
                        "primitive"
                      ),listOfDocs
                    )
                  )
                }
              })
            map=map ++ mc.value
          }
        )
      val explodedDF=
        explodedDF0
          .select(nonArrayFields.map(col):_*)
      val maps=explodedDF.collect.map(r=>scala.collection.mutable.Map(explodedDF.columns.zip(r.toSeq):_*))
      var inboundDocs= ""
      var listOfIds=List[String]()
      maps
        .foreach(
          map=> {
            listOfIds = map.get("_id").get.asInstanceOf[String] :: listOfIds
          }
        )
      val sparkConf=sparkSession.sparkContext.getConf
      val readOverrides=new util.HashMap[String,String]()
      readOverrides.put("spark.mongodb.input.uri","mongouri")
      readOverrides.put("spark.mongodb.input.collection","collection")
      readOverrides.put("spark.mongodb.input.database","db")
      if(listOfIds.nonEmpty){
        listOfIds
          .foreach(
            id  =>{
              inboundDocs+="'"+id+"',"
            }
          )
        inboundDocs=inboundDocs.substring(0,inboundDocs.length-1)
        readOverrides.put("pipeline","[{$match:{_id:{$in:["+inboundDocs+"]}}}]")
      }
      val readConfig=ReadConfig.create(sparkConf,readOverrides)
      val documentsRDD=MongoSpark.load(sparkSession.sparkContext,readConfig)
      documentsRDD.cache()
      //get existing documents from Mongo
      var listOfExistingIds:List[String]=List.empty
      if(!documentsRDD.isEmpty()){
        listOfExistingIds=
          documentsRDD.toDF
            .select("_id")
            .rdd
            .map(_.getString(0))
            .collect()
            .toList
      }

      //load new elements into mongo
      batchDF
        .filter(
          row=>{
            !listOfExistingIds.contains(row.getString(0))
          }
        ).toDF
        .write
        .format("mongo")
        .mode("append")
        .option("spark.mongodb.output.database","db")
        .option("spark.mongodb.output.collection","collection")
        .option("spark.mongodb.output.uri","mongouri")
        .option("spark.mongodb.output.replaceDocument","false")
        .option("replaceDocument","false")
        .save()

      //modify corr. docs from mongo based on inbound events now available in maps
      val concernedRDDs=
        documentsRDD
          .filter(
            document => {
              listOfIds.contains(document.get("_id").asInstanceOf[String])
            }
          )
          .map(
            document=>{
              updateDocumentForNonArrayFields(document,maps)
              updateDocumentForArrayFields(document,map)
              document
            }
          )

      //load modified docs back into mongo
      val writeOverrides=new util.HashMap[String,String]()
      writeOverrides.put("spark.mongodb.output.uri","mongouri")
      writeOverrides.put("spark.mongodb.output.collection","collection")
      writeOverrides.put("spark.mongodb.output.database","database")
      writeOverrides.put("replaceDocument","false")
      val writeConfig=WriteConfig.create(sparkConf,writeOverrides)
      val mongoConnector=MongoConnector(writeConfig.asOptions)
      concernedRDDs
        .foreachPartition{
          partition =>{
            if(partition.nonEmpty){
              mongoConnector.withCollectionDo(
                writeConfig,
                {
                  collection:MongoCollection[Document] => {
                    partition foreach{
                      document => {
                        val searchDocument=new Document()
                        searchDocument.append("_id",document.get("_id").asInstanceOf[String])
                        collection.replaceOne(searchDocument,document)
                      }
                    }
                  }
                }
              )
            }
          }
        }
    }
  }
}
