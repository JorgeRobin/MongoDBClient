package mongoDBClient

import org.joda.time.DateTime

import play.api.data._
import play.api.data.Forms.{ text, longNumber, mapping, nonEmptyText, optional }
import play.api.data.validation.Constraints.pattern

case class AssetFilter(
  id: String,
  aCompanyName: String,
  aServiceName: String,
  aUsername: String,
  aDocumentType: String,
  aFilename:String,
  aExtension:String,
  aFrom: String,
  aTo: String,
  aStatus: String)

// Turn off your mind, relax, and float downstream
// It is not dying...
object AssetFilter {
  import play.api.libs.json._

  implicit object AssetFilterWrites extends OWrites[AssetFilter] {
    def writes(AssetFilter: AssetFilter): JsObject = Json.obj(
      "id" -> AssetFilter.id,
      "aCompanyName" -> AssetFilter.aCompanyName,
      "aServiceName" -> AssetFilter.aServiceName,
      "aUsername" -> AssetFilter.aUsername,
      "aDocumentType" -> AssetFilter.aDocumentType,
      "aFilename" -> AssetFilter.aFilename,
      "aExtension" -> AssetFilter.aExtension,
      "aFrom" -> AssetFilter.aFrom,
      "aTo" -> AssetFilter.aTo,
      "aStatus" -> AssetFilter.aStatus)
  }

  implicit object AssetFilterReads extends Reads[AssetFilter] {
    def reads(json: JsValue): JsResult[AssetFilter] = json match {
      case obj: JsObject => try {
        val id = (obj \ "id").as[String]
        val aCompanyName = (obj \ "aCompanyName").as[String]
        val aServiceName = (obj \ "aServiceName").as[String]
        val aUsername = (obj \ "aUsername").as[String]
        val aDocumentType = (obj \ "aDocumentType").as[String]
        val aFilename = (obj \ "aFilename").as[String]
        val aExtension = (obj \ "aExtension").as[String]
        val aFrom = (obj \ "aFrom").as[String]
        val aTo = (obj \ "aTo").as[String]
        val aStatus = (obj \ "aStatus").as[String]

        JsSuccess(AssetFilter(id, aCompanyName, aServiceName, aUsername, aDocumentType, aFilename, aExtension, aFrom, aTo, aStatus))
        
      } catch {
        case cause: Throwable => JsError(cause.getMessage)
      }

      case _ => JsError("expected.jsobject")
    }
  }

  val form = Form(
    mapping(
      "id" -> text,
      "aCompanyName" -> nonEmptyText,
      "aServiceName" -> nonEmptyText,
      "aUsername" -> nonEmptyText,
      "aDocumentType" -> nonEmptyText,
      "aFilename" -> text,
      "aExtension" -> nonEmptyText,
      "aFrom" -> text,
      "aTo" -> text,
      "aStatus" -> text) {
              (id, aCompanyName, aServiceName, aUsername, aDocumentType, aFilename, aExtension, aFrom, aTo, aStatus) =>
         AssetFilter(
           id,
           aCompanyName,
           aServiceName,
           aUsername,
           aDocumentType,
           aFilename,
           aExtension,
           aFrom,
           aTo,
           aStatus)
                                  } { AssetFilter =>
         Some(
           (AssetFilter.id,
             AssetFilter.aCompanyName,
             AssetFilter.aServiceName,
             AssetFilter.aUsername,
             AssetFilter.aDocumentType,
             AssetFilter.aExtension,
             AssetFilter.aFilename,
             AssetFilter.aFrom,
             AssetFilter.aTo,
             AssetFilter.aStatus))
                                            }
  )
}
