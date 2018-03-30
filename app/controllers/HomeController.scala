package controllers

import akka.routing.BalancingPool
import javax.inject._
import play.api._
import play.api.mvc._
import org.neo4j.driver.v1._
import com.vector._
import play.api.inject.ApplicationLifecycle
import scala.util.Random
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.util.{Failure, Success}

/**
 * This controller creates an `Action` to handle HTTP requests to the
 * application's home page.
 */

import play.api.libs.concurrent.CustomExecutionContext
import akka.actor._

trait MyExecutionContext extends ExecutionContext

class MyExecutionContextImpl @Inject()(system: ActorSystem)
  extends CustomExecutionContext(system, "blocking-pool") with MyExecutionContext



@Singleton
//class HomeController @Inject()(system: ActorSystem, myExecutionContext: MyExecutionContextImpl, cc: ControllerComponents)(implicit exec: ExecutionContext, lifecycle: ApplicationLifecycle) extends AbstractController(cc) {
class HomeController @Inject()(system: ActorSystem, cc: ControllerComponents) (implicit exec: ExecutionContext, lifecycle: ApplicationLifecycle) extends AbstractController(cc)  {

  /**
   * Create an Action to render an HTML page.
   *
   * The configuration in the `routes` file means that this method
   * will be called when the application receives a `GET` request with
   * a path of `/`.
   */

  val blockingEC: ExecutionContext = system.dispatchers.lookup("blocking-pool")
  //val promotionEngine: ActorRef = system.actorOf(BalancingPool(10).props(PromotionEngine.props(ws)), "neoconnect-actor")

  val db = new Neo4j(lifecycle)

  def index() = Action { implicit request: Request[AnyContent] =>
    Ok(views.html.index(""))
  }

  def autogencodigos = Action.async { implicit request: Request[AnyContent] =>
    Future {blocking{
      val codg = 1 to 35 map { _ => "Cod" + Random.nextInt(1000000) }
      val query2 = codg.mkString(",")
      val resultado = db.getAsociatepromotions(codg.toList)
      Ok(views.html.result(resultado._1, resultado._2, query2))
      //Ok(views.html.index(codg.mkString(",")))
    }}(blockingEC)
  }

  def autogencodigosAsync = Action.async { implicit request: Request[AnyContent] =>
    val date = System.currentTimeMillis()
    //println (" Procesando Entrada-- de " + date.toString )
    val codg = 1 to 35 map { _ => "Cod" + Random.nextInt(1000000) }
    val query2 = codg.mkString(",")
    val f = db.getAsociatepromotionsAsync(codg.toList, exec)
    f.map(resultado => {
          val dur2 = System.currentTimeMillis()
          val durd2 = dur2 - date
          //println(" Procesando Resultado de " + date.toString + " en " + durd2.toString)
          Ok(views.html.result(resultado._1, List(durd2.toString), query2))}
          //Ok(views.html.result(Vector(new Promocion(resultado,resultado,1,resultado,resultado,List(resultado) )), List(durd2.toString), query2))}
    )(blockingEC)
      //Ok(views.html.index(codg.mkString(",")))
  }

  def getServicePromo = Action.async { implicit request: Request[AnyContent] =>
    Future { blocking{
    val query2: String = request.getQueryString("input_codes").getOrElse(request.toString())
    val lista: List[String] = query2.split(",").map(_.trim).toList
    val resultado = db.getAsociatepromotions(lista)
    Ok(views.html.result(resultado._1, resultado._2,query2))
    }}(blockingEC)
  }

}