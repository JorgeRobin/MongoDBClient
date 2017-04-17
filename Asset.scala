package mongoDBClient

import org.joda.time.DateTime

import play.api.data._
import play.api.data.Forms.{ text, longNumber, mapping, nonEmptyText, optional }
import play.api.data.validation.Constraints.pattern

case class Asset(
  id: String,
  aCompanyName: String,
  aServiceName: String,
  aUsername: String,
  aDocumentType: String,
  aFilename:String,
  aExtension:String,
  aPictureWidth: String,
  aPictureHeight: String,
  aStatus: String)

// 
object Asset {
  import play.api.libs.json._

  implicit object AssetWrites extends OWrites[Asset] {
    def writes(asset: Asset): JsObject = Json.obj(
      "id" -> asset.id,
      "aCompanyName" -> asset.aCompanyName,
      "aServiceName" -> asset.aServiceName,
      "aUsername" -> asset.aUsername,
      "aDocumentType" -> asset.aDocumentType,
      "aFilename" -> asset.aFilename,
      "aExtension" -> asset.aExtension,
      "aPictureWidth" -> asset.aPictureWidth,
      "aPictureHeight" -> asset.aPictureHeight,
      "aStatus" -> asset.aStatus)
  }

  implicit object AssetReads extends Reads[Asset] {
    def reads(json: JsValue): JsResult[Asset] = json match {
      case obj: JsObject => try {
        val id = (obj \ "id").as[String]
        val aCompanyName = (obj \ "aCompanyName").as[String]
        val aServiceName = (obj \ "aServiceName").as[String]
        val aUsername = (obj \ "aUsername").as[String]
        val aDocumentType = (obj \ "aDocumentType").as[String]
        val aFilename = (obj \ "aFilename").as[String]
        val aExtension = (obj \ "aExtension").as[String]
        val aPictureWidth = (obj \ "aPictureWidth").as[String]
        val aPictureHeight = (obj \ "aPictureHeight").as[String]
        val aStatus = (obj \ "aStatus").as[String]

        JsSuccess(Asset(id, aCompanyName, aServiceName, aUsername, aDocumentType, aFilename, aExtension, aPictureWidth, aPictureHeight, aStatus))
        
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
      "aPictureWidth" -> text,
      "aPictureHeight" -> text,
      "aStatus" -> text) {
              (id, aCompanyName, aServiceName, aUsername, aDocumentType, aFilename, aExtension, aPictureWidth, aPictureHeight, aStatus) =>
         Asset(
           id,
           aCompanyName,
           aServiceName,
           aUsername,
           aDocumentType,
           aFilename,
           aExtension,
           aPictureWidth,
           aPictureHeight,
           aStatus)
                                  } { asset =>
         Some(
           (asset.id,
             asset.aCompanyName,
             asset.aServiceName,
             asset.aUsername,
             asset.aDocumentType,
             asset.aExtension,
             asset.aFilename,
             asset.aPictureWidth,
             asset.aPictureHeight,
             asset.aStatus))
                                            }
  )
}
