package controllers

import javax.inject._
import play.api._
import play.api.mvc._

import scala.concurrent.ExecutionContext
import org.neo4j.driver.v1._
import com.vector._
import play.api.inject.ApplicationLifecycle
import scala.util.Random

/**
 * This controller creates an `Action` to handle HTTP requests to the
 * application's home page.
 */
@Singleton
class HomeController @Inject()(cc: ControllerComponents)(implicit exec: ExecutionContext, lifecycle: ApplicationLifecycle) extends AbstractController(cc) {

  /**
   * Create an Action to render an HTML page.
   *
   * The configuration in the `routes` file means that this method
   * will be called when the application receives a `GET` request with
   * a path of `/`.
   */

  val db = new Neo4j(lifecycle)

  def index() = Action { implicit request: Request[AnyContent] =>
    Ok(views.html.index(""))
  }

  def autogencodigos() = Action { implicit request: Request[AnyContent] =>
    val codg = 1 to 35 map { _ => "Cod" + Random.nextInt(1000000) }
    val query2 = codg.mkString(",")
    val resultado = db.getAsociatepromotions(codg.toList)
    Ok(views.html.result(resultado._1, resultado._2,query2))
    //Ok(views.html.index(codg.mkString(",")))
  }


  def getServicePromo = Action { implicit request: Request[AnyContent] => {
    //Logger.info("Enviado tick - " + tipo + " - tick connected")
    //val lista: List[String] = List ("Cod1", "Cod3","Cod8")
    val query2: String = request.getQueryString("input_codes").getOrElse(request.toString())
    val lista: List[String] = query2.split(",").map(_.trim).toList
    val resultado = db.getAsociatepromotions(lista)
    //Ok(views.html.index(resultado._1 + " en " + resultado._2.toString + " milisec"))
    Ok(views.html.result(resultado._1, resultado._2,query2))
    }
  }

}
