package com.uday.flatten

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat
import java.util.Date

object flatten {
	/* Schema of a schema when converted to json */
    val schemaJson_schema= StructType(Array(
							StructField("fields",ArrayType(StructType(Array(
										StructField("name",StringType,true), 
										StructField("nullable",BooleanType,true), 
										StructField("type",StringType,true))),true),true),
							StructField("type",StringType,true), 
							StructField("name",StringType,false)
							))
	/* to get date in string */
    def getDate(format: String, interval:Integer=0, method:String="add"):String={
	    val sdf=new SimpleDateFormat(format)
		var dt=""
		if(method.isEmpty) { dt=sdf.format(new Date(System.currentTimeMillis())) }
		if(method=="add") { dt=sdf.format(new Date(System.currentTimeMillis() + (interval*1000))) }
		if(method=="sub") { dt=sdf.format(new Date(System.currentTimeMillis() - (interval*1000))) }
	    dt
    }

/*to flatten an Array */
    def arrayFlatten(dfin: DataFrame, flattenArrayString: Boolean= false): DataFrame = {
		val arrayList= if(flattenArrayString) { dfin.schema.fields.filter(_.dataType.simpleString.startsWith("array")).map(_.name) } else {
												dfin.schema.fields.filter(_.dataType.simpleString.startsWith("array<struct")).map(_.name) }
		if(arrayList.size==0){
			dfin
		} else {
			val explodeDF=dfin.withColumn(arrayList(0), explode_outer(col(arrayList(0))))
			arrayFlatten(explodeDF,flattenArrayString)
		}
	}
/*to flatten a column*/
	def columnFlatten(spark: SparkSession, dfin: DataFrame, flattenArrayStruct:Boolean = true, flattenArrayString: Boolean = false): DataFrame = {
		var scm: String = ""
		var dftmp: org.apache.spark.sql.DataFrame = spark.emptyDataFrame
		var columnList: Array[org.apache.spark.sql.Column] = Array()
		
		scm=dfin.schema.json
		dftmp=spark.read.schema(schemaJson_schema).json(spark.sparkContext.parallelize(Seq(scm)).map(_.toString)).withColumn("name",lit(""))
		columnList=getColList(spark,dftmp).map(x=>column(x).as(x.replaceAll("&|:| |-|\\$|/","_").replace(".","")))
		val flat_str_df=dfin.select(columnList:_*)
		val flat_arr_df=  arrayFlatten(flat_str_df, flattenArrayString) 
        val arraycond=if(flattenArrayStruct) { if(flattenArrayString) {(true,"array")} else {(true,"array<struct")}} else {(false,"")}
		if(flat_arr_df.schema.filter(x=>x.dataType.simpleString.contains(arraycond)).map(x=>column(x.name)).size>0) {
			columnFlatten(spark, flat_arr_df)
		} else {
			scm=flat_arr_df.schema.json
			dftmp=spark.read.schema(schemaJson_schema).json(spark.sparkContext.parallelize(Seq(scm)).map(_.toString)).withColumn("name",lit(""))
			columnList=getColList(spark,dftmp).map(x=>column(x).as(x.replaceAll("&|:| |-|\\$|/","_").replace(".","")))
			flat_arr_df.select(columnList:_*)
		}
	}
/*to get list of fields in a json*/	
	def getColList(spark: SparkSession, df:DataFrame): Array[String] = {
		import spark.implicits._
		val cols=df.select($"name",explode($"fields").as("field")).select(when($"name"==="", $"field.name").otherwise(concat($"name",lit("."),$"field.name")).as("name"),$"field.type")
		val collist=cols.filter(when(get_json_object($"type","$.type").isNotNull,get_json_object($"type","$.type")).otherwise($"type")!=="struct").select($"name").collect().map(x=>x(0).toString)
		val df_structList=cols.filter(when(get_json_object($"type","$.type").isNotNull,get_json_object($"type","$.type")).otherwise($"type")==="struct").select($"name",$"type")
		if(df_structList.count==0){
			return collist
		} else {
			val scm1=df.drop("name").schema
			return collist ++ getColList(spark,df_structList.select($"name",from_json($"type",scm1).as("val")).select($"name",$"val.*"))
		}
	}
/*to resolve the issue of duplicate fields*/
	def schemaCorrection(in_inputSchema:DataType):DataType={
		if(in_inputSchema.typeName.toLowerCase=="struct"){
			var output : StructType = new StructType()
			var fieldCount : Map[String, Int] = Map()
			val inputSchema = in_inputSchema.asInstanceOf[StructType]
			for(j<-inputSchema.fields){
				fieldCount = if(fieldCount.keys.toList.contains(j.name.toLowerCase)) {
					fieldCount++Map(j.name.toLowerCase->(fieldCount(j.name.toLowerCase)+1))
				} else {
					fieldCount++Map(j.name.toLowerCase->0)
				}
				val jname=if(fieldCount(j.name.toLowerCase)>0){ j.name+"_"+fieldCount(j.name.toLowerCase) } else { j.name }
				j.dataType.typeName match {
					case "array"  => { output=output.add(jname,ArrayType(schemaCorrection(j.dataType.asInstanceOf[ArrayType].elementType)), j.nullable) }
					case "struct" => { output=output.add(jname, schemaCorrection(j.dataType), j.nullable) }
					case _        => { output=output.add(jname, j.dataType, j.nullable) }
				}
			}
			output
		} else {
			in_inputSchema
		}
	}
	
}