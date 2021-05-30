package com.sheel.classes.accumulator

import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable.Map

class CustomAccumulator extends AccumulatorV2[((String,Boolean,String,String,String),List[Map[String,Any]]),Map[(String,Boolean,String,String,String),List[Map[String,Any]]]]{
  private var myMap:Map[(String,Boolean,String,String,String),List[Map[String,Any]]]=Map.empty

  override def copy() = this

  override def reset(): Unit = myMap

  override def isZero: Boolean = myMap.size==0

  override def add(v: ((String, Boolean, String, String, String), List[Map[String, Any]])): Unit = myMap+=(v._1 -> v._2)

  override def merge(other: AccumulatorV2[((String, Boolean, String, String, String), List[Map[String, Any]]), Map[(String, Boolean, String, String, String), List[Map[String, Any]]]]): Unit = other.value.foreach(add)

  override def value = myMap
}
