package com.vector

import java.util.concurrent.CompletionStage

import org.neo4j.driver.v1._
import org.neo4j.driver.v1.types.Node

import scala.concurrent.{ExecutionContext, Future, blocking}
import play.api.inject.ApplicationLifecycle
import views.html.defaultpages.error

import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap
import scala.collection.mutable
//import scala.concurrent.java8.FuturesConvertersImpl._
import scala.compat.java8.FutureConverters._
import scala.util.{Failure, Success}


object bdNeo4j {
  def url: String = "bolt://localhost/7687"
  def condicion(param: Any):String = param match {
      case param:String => "=\"" + param + "\""
      case param:List[_]  => {
        val list: List[String] = for (a <- param) yield "\"" + a + "\""
        " IN [" + list.mkString(",") + "]"
      }
  }

  def qListPromCodigo(codigo: Any): String = {
    val p1 =      "MATCH (c:Producto)-[*0..]->()<-[r:APLICA_A|:APLICA_PARCIAL_A]-(p:Promocion) USING INDEX c:Producto(name)"
    val p2 = p1 + " WHERE c.name" + condicion(codigo)
    val p3 = p2 + " RETURN DISTINCT p.name AS a, p.tipo AS b, p.numpro AS c, p.desc AS d, type(r) AS e, c.name AS f"
    p3
  }

  def recordToString (record: Record): String = {
    val p1 = record.get("a").asString() + "," + record.get("b").asString() + "," + record.get("c").asInt()
    p1 + "," + record.get("d").asString() + "," + record.get("e").asString() + "," + record.get("f").asString()
  }

  def procesarRecordconMap (result: StatementResult): List[String] = {
    result.asScala
        .toList
        .map(record => record.get("a").asString() + "," + record.get("b").asString() + "," + record.get("c").asInt() + "," + record.get("d").asString() + "," + record.get("e").asString() + "," + record.get("f").asString())
  }

  def procesarRecordconForp (result: StatementResult): List[String] = {
    val list = for (record <- result.asScala) yield {
      val p1 = record.get("a").asString() + "," + record.get("b").asString() + "," + record.get("c").asInt()
      p1 + "," + record.get("d").asString() + "," + record.get("e").asString() + "," + record.get("f").asString()
    }
    list.toList
  }
}



class Neo4j(lifecycle: ApplicationLifecycle){

  case class User(name: String, last_name: String, age: Int, city: String)
  val driver = GraphDatabase.driver(bdNeo4j.url, Config.build().withMaxConnectionPoolSize(100).toConfig)
  //val session = driver.ssession
  lifecycle.addStopHook { () =>
    Future.successful{
      //session.close()
      driver.close()
    }
  }

  def promosParcialesAplicables (record: Vector[Promocion]): Vector[Promocion] = {
    val listapromos: Vector[Promocion] = record.filter(x => x.codigos.length == x.numcod)
    listapromos
  }

  def getAsociatepromotions(codigo: Any): (Vector[Promocion], List[String]) = {

    val script = bdNeo4j.qListPromCodigo(codigo)
    val date = System.currentTimeMillis()
    val session = driver.session
    val result: StatementResult = session.run(script)

    var record_fetch: Vector[Promocion] = Vector()
    var indice_promos = new scala.collection.mutable.HashMap[String,Int]
    var parcial_promo: Vector[Promocion] = Vector()
    var indice_parcial_promos = new scala.collection.mutable.HashMap[String,Int]
    if (result.hasNext()) {
      while (result.hasNext()) {
        val record = result.next()
        val rec_promo = new Promocion( record.get("a").asString(), record.get("b").asString() ,record.get("c").asInt(), record.get("d").asString() ,record.get("e").asString() ,List(record.get("f").asString()))
        if (rec_promo.numcod == 1 ) {
          record_fetch = record_fetch :+ rec_promo
          indice_promos += rec_promo.codpromo -> (record_fetch.length - 1)
        } else {
          if (indice_parcial_promos.exists(_._1 == rec_promo.codpromo)) {
            var promotmp = parcial_promo(indice_parcial_promos(rec_promo.codpromo))
            promotmp.codigos = promotmp.codigos ::: rec_promo.codigos
          } else {
            parcial_promo = parcial_promo :+ rec_promo
            indice_parcial_promos += rec_promo.codpromo -> (parcial_promo.length - 1)
          }
        }
      }
    }else{
      "Ningún acierto"
    }
    record_fetch = promosParcialesAplicables(parcial_promo) ++ record_fetch
    val dur2 = System.currentTimeMillis()
    val durd2 = dur2 - date
    session.close()
    (record_fetch, List(durd2.toString ))
  }

  def getAsociatepromotionsAsync (codigo: Any, bc: ExecutionContext): Future[(Vector[Promocion], List[String])] = {
    //implicit val ec: ExecutionContext = bc
    val script = bdNeo4j.qListPromCodigo(codigo)
    val session = driver.session
    val cursorStage: CompletionStage[StatementResultCursor]  = session.runAsync(script)

    val result: CompletionStage[Record] = cursorStage.thenCompose(f => f.nextAsync())

      val salida: CompletionStage[(Vector[Promocion], List[String])] = result.thenApply( (record: Record) => {
      val record_fetch = Vector(new Promocion(record.get("a").asString(), record.get("b").asString(), record.get("c").asInt(), record.get("d").asString(), record.get("e").asString(), List(record.get("f").asString())))
      //println (" Procesando CompletionStage--- de " + date.toString + " en " + durd2.toString)
      session.closeAsync()
      (record_fetch, List(2.toString))
    })
    toScala(salida)

  }

  def getAsociatepromotionsAsync1 (codigo: Any, bc: ExecutionContext): Future[(Vector[Promocion], List[String])] = {
    implicit val ec: ExecutionContext = bc
    val script = bdNeo4j.qListPromCodigo(codigo)

    val session = driver.session
    val cursorStage: CompletionStage[StatementResultCursor]  = session.runAsync(script)
    val cursorFuture = toScala(cursorStage)

    //val result = cursorFuture.foreach(r => r.nextAsync())
    val result1 = cursorFuture.flatMap(f => toScala(f.listAsync()))(bc)

    val salida = result1.map {
      case result3 =>
        val result = result3.listIterator()

        var record_fetch: Vector[Promocion] = Vector()
        var indice_promos = new scala.collection.mutable.HashMap[String, Int]
        var parcial_promo: Vector[Promocion] = Vector()
        var indice_parcial_promos = new scala.collection.mutable.HashMap[String, Int]
        if (result.hasNext()) {
          while (result.hasNext()) {
            val record = result.next()
            val rec_promo = new Promocion(record.get("a").asString(), record.get("b").asString(), record.get("c").asInt(), record.get("d").asString(), record.get("e").asString(), List(record.get("f").asString()))
            if (rec_promo.numcod == 1) {
              record_fetch = record_fetch :+ rec_promo
              indice_promos += rec_promo.codpromo -> (record_fetch.length - 1)
            } else {
              if (indice_parcial_promos.exists(_._1 == rec_promo.codpromo)) {
                var promotmp = parcial_promo(indice_parcial_promos(rec_promo.codpromo))
                promotmp.codigos = promotmp.codigos ::: rec_promo.codigos
              } else {
                parcial_promo = parcial_promo :+ rec_promo
                indice_parcial_promos += rec_promo.codpromo -> (parcial_promo.length - 1)
              }
            }
          }
        } else {
          "Ningún acierto"
        }
        record_fetch ++= promosParcialesAplicables(parcial_promo)
        session.closeAsync()
        (record_fetch, List(2.toString))
    }(bc)
    salida
  }

  def insertRecord(user: User): Int = {
    val driver = GraphDatabase.driver("bolt://localhost/7687", AuthTokens.basic("anurag", "@nurag06"))
    val session = driver.session
    val script = s"CREATE (user:Users {name:'${user.name}',last_name:'${user.last_name}',age:${user.age},city:'${user.city}'})"
    val result: StatementResult = session.run(script)
    session.close()
    driver.close()
    result.consume().counters().nodesCreated()
  }

  def retrieveRecord(name: String) : String= {
    val driver = GraphDatabase.driver("bolt://localhost/7687", AuthTokens.basic("anurag", "@nurag06"))
    val session = driver.session
    val script = s"MATCH (a:Users) WHERE a.name = '$name' RETURN a.name AS name, a.last_name AS last_name, a.age AS age, a.city AS city"
    val result = session.run(script)
    val record_data = if (result.hasNext()) {
      val record = result.next()
      println(record.get("name").asString() + " " + record.get("last_name").asString() + " " + record.get("age").asInt() + " " + record.get("city").asString())
      record.get("name").asString()
    }else{
      s"$name not found."
    }
    session.close()
    driver.close()
    record_data
  }

  def retrieveAllRecord(): Boolean = {
    val driver = GraphDatabase.driver("bolt://localhost/7687", AuthTokens.basic("anurag", "@nurag06"))
    val session = driver.session
    val script = s"MATCH (user:Users) RETURN user.name AS name, user.last_name AS last_name, user.age AS age, user.city AS city"
    val result: StatementResult = session.run(script)
    val record_fetch = if (result.hasNext()) {
      val record = result.next()
      println(record.get("name").asString() + " " + record.get("last_name").asString() + " " + record.get("age").asInt() + " " + record.get("city").asString())
      true
    }else{
      false
    }
    session.close()
    driver.close()
    record_fetch
  }

  def updateRecord(name: String, newName: String): Boolean = {
    val driver = GraphDatabase.driver("bolt://localhost/7687", AuthTokens.basic("anurag", "@nurag06"))
    val session = driver.session
    val script = s"MATCH (user:Users) where user.name ='$name' SET user.name = '$newName' RETURN user.name AS name, user.last_name AS last_name," +
      s" user.age AS age, user.city AS city"
    val result = session.run(script)
    session.close()
    driver.close()
    result.consume().counters().containsUpdates()
  }

  def deleteRecord(name: String): Int = {
    val driver = GraphDatabase.driver("bolt://localhost/7687", AuthTokens.basic("anurag", "@nurag06"))
    val session = driver.session
    val script = s"MATCH (user:Users) where user.name ='$name' Delete user"
    val result = session.run(script)
    session.close()
    driver.close()
    result.consume().counters().nodesDeleted()
  }

  def createNodesWithRelation(user_name: String, userList: List[String], relation: String) = {
    val driver = GraphDatabase.driver("bolt://localhost/7687", AuthTokens.basic("anurag", "@nurag06"))
    val session = driver.session
    val nameOfFriends = "\"" + userList.mkString("\", \"") + "\""
    val script = s"MATCH (user:Users {name: '$user_name'}) FOREACH (name in [$nameOfFriends] | CREATE (user)-[:$relation]->(:Users {name:name}))"
    val result = session.run(script)
    session.close()
    driver.close()
    result.consume().counters().relationshipsCreated()
  }

  def fetchFriendsOfFriends(user_name: String, relation: String): String = {
    val driver = GraphDatabase.driver("bolt://localhost/7687", AuthTokens.basic("anurag", "@nurag06"))
    val session = driver.session
    val script = s"MATCH (user:Users)-[:$relation]-(friend:Users)-[:$relation]-(foaf:Users) WHERE user.name = '$user_name' AND NOT (user)-[:$relation]-(foaf) RETURN foaf.name As name"
    val result = session.run(script)
    val name_of_friend_of_friend = if(result.hasNext()) {
      val record = result.next()
      record.get("name").asString()
    }else{
      "No friends found."
    }
    session.close()
    driver.close()
    name_of_friend_of_friend
  }
}