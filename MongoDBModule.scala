package mongoDBClient;
import java.io.{ File, FileOutputStream, FileInputStream }

import play.api.libs.iteratee._
import play.api.Play.current
import play.api.libs.json._
import play.api.libs.functional.syntax._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{ Failure, Success }
import scala.collection.mutable.MutableList
import scala.collection.mutable.HashSet
import scala.collection.mutable.LinkedHashSet
import scala.collection.immutable.ListMap
import scala.util.control.Breaks._

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException
import com.mongodb.ParallelScanOptions;
import com.mongodb.ServerAddress
import com.mongodb.MongoCredential
import com.mongodb.MongoClientOptions
import com.mongodb.Mongo

import java.io.FileOutputStream
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.IOException
import java.io.FileNotFoundException
import java.io.InputStream
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream
import java.util.HashMap
import java.util.Date
import java.util.Calendar
import java.text.SimpleDateFormat
import java.text.DateFormat
import play.api.Logger
import play.cache.Cache

import neo4j.services.uman.UserManagementService
import utils._

object MongoDBModule {
  // change this line to your temporary storage path
  var filePath = System.getProperty("user.dir") + "/artifacts/assets/"
  // 
  val mongodbServer = "yourMongoDBIP_Instance"
  val mongodbPort = 27017                       // default port
  val mongodbUsername = "yourusername"
  val mongodbPassword = "yourpassword"
 
  val logger:Logger = Logger("uCommon")
  var mongoOk = true
  var db:DB = null
  val credential = MongoCredential.createCredential(mongodbUsername, "admin", mongodbPassword.toArray);
  var mongoOptions = MongoClientOptions.builder().connectTimeout(5000)
  val serverAddress = new ServerAddress(mongodbServer, mongodbPort) 
  val uri = new MongoClientURI("mongodb://" + mongodbUsername + ":" + mongodbPassword + "@" +  mongodbServer + "/" + "assets", mongoOptions)
  val mongoClient = new MongoClient(uri)
  
  if (mongoClient != null) {
    try {
      // assuming a Database assets
       db = mongoClient.getDB("assets")
       val databaseNames = db.getCollectionNames
       logger.info(" Connected successfully to the mongoDB server: " + mongodbServer + ":" + mongodbPort + " database: " + db.getName())
    } catch {
       case (e:Exception) => {
         if (e.getMessage().contains("Authentication failed")) {
           logger.error("MongoDB - Authentication failed - invalid credentials - contact system administrator")
         } else {
           logger.error("MongoDB is down - check cloud instance or contact system administrator: " + e.getMessage())
         }
         mongoClient.close()
         mongoOk = false
       }
    }
  } else {
    logger.error("MongoDBModule - mongoClient null - see previous exception")
    mongoOk = false;
  }

  // store document in mongodb from file system
  def storeDocument (asset:Asset, filenamePath:String, filename:String, contentType:String): StatusCode = {
    val statusCode = new StatusCode(200)
    if (mongoOk) {
      logger.info("storeDocument: started filenamePath " + filenamePath + filename)
      var gridDoc = new com.mongodb.gridfs.GridFS(db)
      val file  = new File(filenamePath + filename) 
      if (file.exists()) {
        logger.info("storeDocument - filename on: " + file + " length: " + file.length())
        val fi:FileInputStream = new FileInputStream(file) 
        val in:com.mongodb.gridfs.GridFSInputFile = gridDoc.createFile(fi)
        in.put("id", asset.id)
        in.put("aCompanyName", asset.aCompanyName)
        in.put("aServiceName", asset.aServiceName)
        in.put("aUsername", asset.aUsername)
        in.put("aDocumentType", asset.aDocumentType)
        in.put("aContentType", contentType)
        var fn = ""
        if (asset.aFilename.equals("")){
          in.put("aFilename", filename)
          fn = filename
        } 
        else {
          in.put("aFilename", asset.aFilename)
          fn = asset.aFilename
        }                           
        in.put("aExtension", asset.aExtension)
        in.put("aPictureWidth", asset.aPictureWidth)
        in.put("aPictureHeight", asset.aPictureHeight)
        in.put("aStatus", asset.aStatus)
        // create meta data
        val md = new BasicDBObject()
        md.put("aDisplayName", fn)
        md.put("aContentType", contentType)
        in.setMetaData(md)
        //
        in.save()
        
        statusCode.message = in.getId().toString
        logger.debug("storeDocument -- stored document successfully : " + fn)
      } else {
        statusCode.status = 404
        statusCode.error = "storeDocument - File not found while storing - file path: " + filenamePath + filename
        statusCode.message = "Check upload process"
        logger.error("storeDocument -- could not find file:" + filenamePath + filename + " check upload process (404)")
      }
      mongoClient.close();
    } else {
      statusCode.status = 401
      statusCode.error = "mongoDB is down - contact system administrator"
      statusCode.message = "Verify if mongoDB is running"
      logger.error("storeDocument -- store document failed: mongoDB is down (401)")
    }
    
    statusCode
  }
  // store document in mongodb from input stream
   def storeDocumentFileStream (asset:Asset, fileStream:java.io.InputStream, filename:String, contentType:String): StatusCode = {
    val statusCode = new StatusCode(200)
     if (mongoOk) {
       logger.info("storeDocumentFileStream: started")
       try {
         var gridDoc = new com.mongodb.gridfs.GridFS(db)
   
         val in:com.mongodb.gridfs.GridFSInputFile = gridDoc.createFile(fileStream)
         in.put("id", asset.id)
         in.put("aCompanyName", asset.aCompanyName)
         in.put("aServiceName", asset.aServiceName)
         in.put("aUsername", asset.aUsername)
         in.put("aDocumentType", asset.aDocumentType)
         in.put("aContentType", contentType)
         var fn = ""
         if (asset.aFilename.equals("")){
           in.put("aFilename", filename)
           fn = filename
         } 
         else {
           in.put("aFilename", asset.aFilename)
           fn = asset.aFilename
         }    
         in.put("aExtension", asset.aExtension)
         in.put("aPictureWidth", asset.aPictureWidth)
         in.put("aPictureHeight", asset.aPictureHeight)
         in.put("aStatus", asset.aStatus)
         // create meta data
         val md = new BasicDBObject()
         md.put("aDisplayName", fn)
         md.put("aContentType", contentType)
         in.setMetaData(md)
         //
         in.save()
         statusCode.message = in.getId().toString
         // put document in play cache
         logger.debug("storing (2) " + fn + " in play cache")
         Cache.set(fn, fileStream)
         logger.debug("storeDocumentFileStream -- stored document file stream successfully : " + fn)
       } catch {
         case e:MongoException => {
           logger.error("storeDocumentFileStream - mongo db error: " + e.getStackTrace())
           statusCode.status = 400
           statusCode.error = "storeDocumentFileStream - mongoDB exception - see logs"
           statusCode.message = "Verify request parameters"
         }
         case e1:Exception => {
           logger.error("storeDocumentFileStream - general exception: " + e1.getStackTrace())
           statusCode.status = 400
           statusCode.error = "storeDocumentFileStream - general exception - see logs"
           statusCode.message = "Verify request parameters"
         }
       }
       mongoClient.close();
     } else {
       statusCode.status = 503
       statusCode.error = "mongoDB is down - contact system administrator"
       statusCode.message = "Verify if mongoDB is running"
     }
    statusCode
  }
  // retrieve document from mongodb by id
  def retrieveDocument (id:String): String = {
    
     var result = ""
     if (mongoOk) {
       logger.info("retrieveDocument: started")
       var gridDoc = new com.mongodb.gridfs.GridFS(db)
       val query = new BasicDBObject("id", id)
       try {
         val doc:com.mongodb.gridfs.GridFSDBFile = gridDoc.findOne(query);
         //println("aExtension=" + doc.get("aExtension"))
         val filename = doc.get("aCompanyName").toString() + doc.get("aServiceName").toString() + doc.get("aUsername").toString()
         doc.writeTo(filePath + filename + "." + doc.get("aExtension"))
         result = filePath + filename + "." + doc.get("aExtension")
         logger.debug("retrieveDocument -- retrieved document successfully : " + filename)
       } catch {
         case ex: MongoException => {
         logger.error(ex.getMessage())
         }
         case ex: Exception => {
         logger.error(ex.getMessage())
         }
       }
       mongoClient.close()
     }
    result
  } 
 
  // retrieve document from mongodb by filename
  def retrieveDocumentByFilename (name:String): String = {
    
    var result = ""
    if (mongoOk) {
      logger.info("retrieveDocumentByFilename: started database " + db.getName())
             
       var gridDoc = new com.mongodb.gridfs.GridFS(db)
       val query = new BasicDBObject("aFilename", name)
       try {
         //val doc:com.mongodb.gridfs.GridFSDBFile = gridDoc.findOne(query);
         // find all documents matching query
         val docs:java.util.List[com.mongodb.gridfs.GridFSDBFile] = gridDoc.find(query);
         var initialFile:com.mongodb.gridfs.GridFSDBFile = docs.get(0);
         var latestIndex = 0
         //println("doc 0:" +  docs.get(0).toString() + " aqui:" + docs.get(0).get("uploadDate").toString() + " size:" + docs.size());
         if (docs.size() > 1) {
           // pick the latest one
           for (i <- 1 to docs.size() - 1) {
             val oneFile:com.mongodb.gridfs.GridFSDBFile = docs.get(i);
             if (oneFile.get("uploadDate").toString() > initialFile.get("uploadDate").toString()) latestIndex = i
             initialFile = oneFile
             //println("i:" + i + " id:" + oneFile.get("aCompanyName").toString() + " aFilename:" + oneFile.get("aFilename").toString())
           }
         }
      
         val doc:com.mongodb.gridfs.GridFSDBFile = docs.get(latestIndex)
         //println("doc latestIndex:" +  docs.get(latestIndex).toString() + " aqui:" + docs.get(latestIndex).get("uploadDate").toString() + " size:" + docs.size());
         //println("aExtension=" + doc.get("aExtension"))
         val filename = doc.get("aCompanyName").toString() + doc.get("aServiceName").toString() + doc.get("aUsername").toString()
         val filenameNoSpaces = filename.replaceAll("\\s+","")
         logger.info("MongoDBModule - filepath + file:" + filePath + filenameNoSpaces + "." + doc.get("aExtension"))
      
         doc.writeTo(filePath + filenameNoSpaces + "." + doc.get("aExtension"))
         result = filePath + filenameNoSpaces + "." + doc.get("aExtension")
         logger.debug("retrieveDocumentByFilename -- retrieved document successfully : ")
       } catch {
           case ex: MongoException => {
             logger.error("retrieveDocumentByfilename (mongo exception): " + ex.getMessage()) 
           }
           case e1: Exception => {
             logger.warn("retrieveDocumentByfilename: file not found " + e1.getMessage())
           }
       }
    }
    mongoClient.close()
    result
  } 
  // retrieve input stream from file name
  def retrieveInputStreamByFilename (name:String): java.io.InputStream = {
    
    var result:java.io.InputStream = null
    if (mongoOk) {
      logger.info("retrieveInputStreamByFilename: started")
        // see if object is in the play cache
        var gridDoc = new com.mongodb.gridfs.GridFS(db)
        val query = new BasicDBObject("aFilename", name)
        try {
           val docs:java.util.List[com.mongodb.gridfs.GridFSDBFile] = gridDoc.find(query);
           var initialFile:com.mongodb.gridfs.GridFSDBFile = docs.get(0);
           var latestIndex = 0
           //println("doc:" +  docs.get(0).toString() + " aqui:" + docs.get(0).get("uploadDate").toString() + " size:" + docs.size());
           if (docs.size() > 1) {
             // pick the latest one
             for (i <- 1 to docs.size() - 1) {
               val oneFile:com.mongodb.gridfs.GridFSDBFile = docs.get(i);
               if (oneFile.get("uploadDate").toString() > initialFile.get("uploadDate").toString()) latestIndex = i
               initialFile = oneFile
               //println("i:" + i + " id:" + oneFile.get("aCompanyName").toString() + " aFilename:" + oneFile.get("aFilename").toString())
             }
           }
           val doc:com.mongodb.gridfs.GridFSDBFile = docs.get(latestIndex)
           result = doc.getInputStream
           logger.debug("retrieveInoutStreamByFilename -- retrieved inout stream successfully : ")
        } catch {
            case ex: MongoException => {
              logger.error(ex.getMessage())
            }
            case ex: Exception => {
              logger.error(ex.getMessage())
            }
        }
 
    } 
    mongoClient.close()
    result
  } 
   
  // get all documents from company name
  def getAllDocumentsCompany (companyName:String): String = {
    var result = "["
    
    var gridDoc = new com.mongodb.gridfs.GridFS(db)
    var query = new BasicDBObject("aCompanyName", companyName)
    if (companyName.equals("")) query = new BasicDBObject()
    val docs:java.util.List[com.mongodb.gridfs.GridFSDBFile] = gridDoc.find(query);
    val it:java.util.Iterator[com.mongodb.gridfs.GridFSDBFile] = docs.iterator()
    var has= false
    while (it.hasNext()) {
      val doc:com.mongodb.gridfs.GridFSDBFile = it.next()
      result += doc + ","
      has = true
    }
    if (has) {
      result = result.substring(0, result.length()-1) + "]"
    } else {
      result = "404"
    }
    
     result
  }
  // query documents filtering by AssetFilter object
  def queryDocuments (asset:AssetFilter): String = {
    var result = "["
    
    if (mongoOk) {
      logger.info("getAllDocumentsCompany: started")
       var gridDoc = new com.mongodb.gridfs.GridFS(db)
       //val qM:java.util.Map[String, String] = new java.util.HashMap()
       val qM = new BasicDBObject()
       if (!asset.id.equals("")) qM.put("id", asset.id)
       if (!asset.aCompanyName.equals("")) qM.put("aCompanyName", asset.aCompanyName)
       if (!asset.aServiceName.equals("")) qM.put("aServiceName", asset.aServiceName)
       if (!asset.aUsername.equals("")) qM.put("aUsername", asset.aUsername)
       if (!asset.aDocumentType.equals("")) qM.put("aDocumentType", asset.aDocumentType)
       if (!asset.aFilename.equals("")) qM.put("aFilename", asset.aFilename)
       if (!asset.aExtension.equals("")) qM.put("aExtension", asset.aExtension)
       val docs:java.util.List[com.mongodb.gridfs.GridFSDBFile] = gridDoc.find(qM)
       val it:java.util.Iterator[com.mongodb.gridfs.GridFSDBFile] = docs.iterator()
       val from = asset.aFrom
       val to = asset.aTo
       while (it.hasNext()) {
         breakable {
           val doc:com.mongodb.gridfs.GridFSDBFile = it.next()
           val mongoDate = new Date(doc.get("uploadDate").toString())
           val df:DateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
           val asISO = df.format(mongoDate)
           //println ("uploadDate:" + asISO + " from:" + from + " to:" + to)
           if (asISO < from || asISO > to) break
           //println(doc.toString())
           result += doc + ","
         }
       }
    }
    result = result.substring(0, result.length()-1) + "]"
    if (result.equals("[") || result.equals("]") || result.equals("[]")) result = ""
    mongoClient.close()
    result
  }
  
  // get all photos from an asset in a ZIP file  
  def getZIPPhotos (asset:Asset): String = {
    var result = filePath + "photos.zip"
    if (mongoOk) {
      logger.info("getZIPPhotos: started")
       val n = 100
       var gridDoc = new com.mongodb.gridfs.GridFS(db)
       //val qM:java.util.Map[String, String] = new java.util.HashMap()
       val qM = new BasicDBObject()
       if (!asset.id.equals("")) qM.put("id", asset.id)
       if (!asset.aCompanyName.equals("")) qM.put("aCompanyName", asset.aCompanyName)
       if (!asset.aServiceName.equals("")) qM.put("aServiceName", asset.aServiceName)
       if (!asset.aUsername.equals("")) qM.put("aUsername", asset.aUsername)
       if (!asset.aDocumentType.equals("")) qM.put("aDocumentType", asset.aDocumentType)
       if (!asset.aFilename.equals("")) qM.put("aFilename", new BasicDBObject("$gt", ""))
    
       var filterFilename = new BasicDBObject("aFilename", asset.aFilename) 
    
       val docs:java.util.List[com.mongodb.gridfs.GridFSDBFile] = gridDoc.find(qM)
       val it:java.util.Iterator[com.mongodb.gridfs.GridFSDBFile] = docs.iterator()
       // do filtering
       val files:MutableList[String] = MutableList()
       val filesStream:MutableList[InputStream] = MutableList()
       var i = 0
       while (it.hasNext()) {
         val doc:com.mongodb.gridfs.GridFSDBFile = it.next()
         //println(doc.toString())
         val fileName = doc.get("aFilename").toString()
         val tempFile = filePath + fileName
         //doc.writeTo(tempFile)
         files += tempFile
         // instead of writing to file system
         filesStream += doc.getInputStream()
       }
       //Zipper(result, files, false)
       ZipperStream(result, files, filesStream, false)
       logger.debug("getZIPPhotos -- got zip successfully")
    } else {
      result = ""
    }
    mongoClient.close()
    result
  }
  // get latest photos in a ZIP file based on AssetFilter variables
  def getZIPLatestPhotos (asset:AssetFilter): String = {
    var result = filePath + "latestPhotos.zip"
    if (mongoOk) {
      logger.info("getZIPLatestPhotos: started")
       val n = 100
       var gridDoc = new com.mongodb.gridfs.GridFS(db)
       //val qM:java.util.Map[String, String] = new java.util.HashMap()
       val qM = new BasicDBObject()
       
       if (!asset.id.equals("")) qM.put("id", asset.id)
       if (!asset.aCompanyName.equals("")) qM.put("aCompanyName", asset.aCompanyName)
       if (!asset.aServiceName.equals("")) qM.put("aServiceName", asset.aServiceName)
       if (!asset.aUsername.equals("")) qM.put("aUsername", asset.aUsername)
       if (!asset.aDocumentType.equals("")) qM.put("aDocumentType", asset.aDocumentType)
       if (!asset.aFilename.equals("")) qM.put("aFilename", new BasicDBObject("$regex", ".*" + asset.aFilename + ".*"))
    
       val docs:java.util.List[com.mongodb.gridfs.GridFSDBFile] = gridDoc.find(qM)
       val it:java.util.Iterator[com.mongodb.gridfs.GridFSDBFile] = docs.iterator()
       // 
       val filesAndStream:java.util.TreeMap[String, com.mongodb.gridfs.GridFSDBFile] = new java.util.TreeMap[String, com.mongodb.gridfs.GridFSDBFile](new EmailComparator())
       //val filesAndStream:java.util.TreeMap[String, com.mongodb.gridfs.GridFSDBFile] = new java.util.TreeMap[String, com.mongodb.gridfs.GridFSDBFile]()
       val from = asset.aFrom
       val to = asset.aTo
       //println("Docs size:" + docs.size())
       // filter by period
       while (it.hasNext()) {
         breakable {
           val doc:com.mongodb.gridfs.GridFSDBFile = it.next()
        
           val mongoDate = new Date(doc.get("uploadDate").toString())
           val df:DateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
           val asISO = df.format(mongoDate)
           if (asISO < from || asISO > to || doc.get("aFilename") == null) break
           //println ("uploadDate:" + asISO + " from:" + from + " to:" + to + " aDocumentType" + doc.get("aDocumentType"))
           // get latest visitor photo
           
           var fileName = doc.get("aFilename").toString()
          
           val email = fileName.substring(0, fileName.lastIndexOf("@") - 1)
           //println("fileName:" + fileName + " email:" + email)
           val tempFile = filePath + fileName.toLowerCase()
           filesAndStream.put(tempFile, doc)
         
         }
       }
       //println("TreeMap size:" + filesAndStream.size)
       // get descending map by email
       val descendingMap:java.util.Map[String, com.mongodb.gridfs.GridFSDBFile] = filesAndStream.descendingMap();
    
       val files:java.util.LinkedList[String] = new java.util.LinkedList()
       val filesDoc:java.util.LinkedList[com.mongodb.gridfs.GridFSDBFile] = new java.util.LinkedList()
       val emailControl:java.util.HashSet[String] = new java.util.HashSet()
       val entries = descendingMap.entrySet().iterator()
       // add only unique emails
       while (entries.hasNext()) {
         breakable {
           val entry = entries.next()
           val doc = entry.getValue
           val fileName = doc.get("aFilename").toString()
           val email = fileName.substring(0, fileName.lastIndexOf("@") - 1)
           val tempDoc = getLatestPhotoDocument(email)
        
           if (tempDoc.toString.equals("")) break
           // control hashset adding
           if (emailControl.add(email)) {
              val fileTemp = filePath + tempDoc.get("aFilename").toString()
              files.add(fileTemp)
              filesDoc.add(tempDoc)
           } else {
             //println("duplicated entry:" + email)
           }
       
         }
      
       }
       //println("before Zip files:" + files.size + " docs:" + filesDoc.size) 
       //Zipper(result, files, false)
       ZipperStreamLinkedHashSet(result, files, filesDoc, false)
       logger.debug("getZIPLatestPhotos -- got ZIP successfully : ")
    } else {
      result = ""
    }
    mongoClient.close()
    result
  }
  // get documents in a period of time  
  // aFrom = from date (yyyy-dd-mmTHH:MM:SS)
  // aTo = to date (yyyy-dd-mmTHH:MM:SS)
  def getZIPDocumentsPeriod (asset:AssetFilter): String = {
    var result = filePath + "documentsPeriod.zip"
    if (mongoOk) {
      logger.info("getZIPDocumentsPeriod: started")
       val n = 100
       var gridDoc = new com.mongodb.gridfs.GridFS(db)
       //val qM:java.util.Map[String, String] = new java.util.HashMap()
       val qM = new BasicDBObject()
       if (!asset.id.equals("")) qM.put("id", asset.id)
       if (!asset.aCompanyName.equals("")) qM.put("aCompanyName", asset.aCompanyName)
       if (!asset.aServiceName.equals("")) qM.put("aServiceName", asset.aServiceName)
       if (!asset.aUsername.equals("")) qM.put("aUsername", asset.aUsername)
       if (!asset.aDocumentType.equals("")) qM.put("aDocumentType", asset.aDocumentType)
       if (!asset.aFilename.equals("")) qM.put("aFilename", new BasicDBObject("$gt", ""))
    
       var filterFilename = new BasicDBObject("aFilename", asset.aFilename) 
    
       val docs:java.util.List[com.mongodb.gridfs.GridFSDBFile] = gridDoc.find(qM)
       val it:java.util.Iterator[com.mongodb.gridfs.GridFSDBFile] = docs.iterator()
       // do filtering
       val files:MutableList[String] = MutableList()
       val filesStream:MutableList[InputStream] = MutableList()
       val from = asset.aFrom
       val to = asset.aTo
       while (it.hasNext()) {
         breakable {
           val doc:com.mongodb.gridfs.GridFSDBFile = it.next()
           //println(doc.toString())
           // filter by time
           val mongoDate = new Date(doc.get("uploadDate").toString())
           val df:DateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
           val asISO = df.format(mongoDate)
           //println ("uploadDate:" + asISO + " from:" + from + " to:" + to)
           if (asISO < from || asISO > to) break
           //println(doc.toString())
           val fileName = doc.get("aFilename").toString()
           val tempFile = filePath + fileName
           //doc.writeTo(tempFile)
           files += tempFile
           // instead of writing to file system
           filesStream += doc.getInputStream()
         }
     
       }
       //Zipper(result, files, false)
       ZipperStream(result, files, filesStream, false)
       logger.debug("getZIPDocumentsPeriod -- got ZIP successfully : ")
    } else {
      result = ""
    }
    mongoClient.close()
    result
  }
  // get Latest Photo Document as a GridFSBFile
  def getLatestPhotoDocument (name:String): com.mongodb.gridfs.GridFSDBFile = {
    var doc:com.mongodb.gridfs.GridFSDBFile = new com.mongodb.gridfs.GridFSDBFile()
    if (mongoOk) {
      logger.info("getLatestPhotoDocument: started")
       var gridDoc = new com.mongodb.gridfs.GridFS(db)
       val query = new BasicDBObject()
       query.put("aDocumentType", "visitor_photo")
       query.put("aFilename", new BasicDBObject("$regex", ".*" + name + ".*"))
       try {
         val docs:java.util.List[com.mongodb.gridfs.GridFSDBFile] = gridDoc.find(query);
         var initialFile:com.mongodb.gridfs.GridFSDBFile = docs.get(0);
         var latestIndex = 0
         //println("doc:" +  docs.get(0).toString() + " aqui:" + docs.get(0).get("uploadDate").toString() + " size:" + docs.size());
         if (docs.size() > 1) {
           // pick the latest one
           for (i <- 1 to docs.size() - 1) {
             val oneFile:com.mongodb.gridfs.GridFSDBFile = docs.get(i);
             if (oneFile.get("uploadDate").toString() > initialFile.get("uploadDate").toString()) latestIndex = i
             initialFile = oneFile
             //println("i:" + i + " id:" + oneFile.get("aCompanyName").toString() + " aFilename:" + oneFile.get("aFilename").toString())
           }
         }
         doc = docs.get(latestIndex)
         logger.debug("getLatestPhotoDocument -- got latest successfully : ")
       } catch {
         case ex: MongoException => {
           logger.error(ex.getMessage())
           doc = null
         }
         case ex: Exception => {
           logger.error(ex.getMessage())
           doc = null
         }
       }
    } else {
      doc = null
    }
    mongoClient.close()
    doc
  } 
  // get Latest Photo Document as a file stream
  def getLatestPhotoFilestream (name:String): java.io.InputStream = {
    
    var result:java.io.InputStream = null
    if (mongoOk) {
      logger.info("getLatestVisitorPhotoFilestream: started")
       var doc:com.mongodb.gridfs.GridFSDBFile = new com.mongodb.gridfs.GridFSDBFile()
       var gridDoc = new com.mongodb.gridfs.GridFS(db)
       val query = new BasicDBObject()
       query.put("aDocumentType", "visitor_photo")
       query.put("aFilename", new BasicDBObject("$regex", ".*" + name + ".*"))
       try {
         val docs:java.util.List[com.mongodb.gridfs.GridFSDBFile] = gridDoc.find(query);
         var initialFile:com.mongodb.gridfs.GridFSDBFile = docs.get(0);
         var latestIndex = 0
         //println("doc:" +  docs.get(0).toString() + " aqui:" + docs.get(0).get("uploadDate").toString() + " size:" + docs.size());
         if (docs.size() > 1) {
           // pick the latest one
           for (i <- 1 to docs.size() - 1) {
             val oneFile:com.mongodb.gridfs.GridFSDBFile = docs.get(i);
             if (oneFile.get("uploadDate").toString() > initialFile.get("uploadDate").toString()) latestIndex = i
             initialFile = oneFile
             //println("i:" + i + " id:" + oneFile.get("aCompanyName").toString() + " aFilename:" + oneFile.get("aFilename").toString())
           }
         }
         doc = docs.get(latestIndex)
         result = doc.getInputStream
         logger.debug("getLatestPhotoFilestream -- got latest successfully : ")
       } catch {
         case ex: MongoException => {
           logger.error(ex.getMessage())
         }
         case ex: Exception => {
           logger.error(ex.getMessage())
         }
       }
    }
    mongoClient.close()
    result
  } 
    // count documents  
  def countDocuments (asset:Asset): String = {
    var result = "0"
    if (mongoOk) {
      logger.info("countDocuments: started")
       var gridDoc = new com.mongodb.gridfs.GridFS(db)
       //val qM:java.util.Map[String, String] = new java.util.HashMap()
       val qM = new BasicDBObject()
       if (!asset.id.equals("")) qM.put("id", asset.id)
       if (!asset.aCompanyName.equals("")) qM.put("aCompanyName", asset.aCompanyName)
       if (!asset.aServiceName.equals("")) qM.put("aServiceName", asset.aServiceName)
       if (!asset.aUsername.equals("")) qM.put("aUsername", asset.aUsername)
       if (!asset.aDocumentType.equals("")) qM.put("aDocumentType", asset.aDocumentType)
       if (!asset.aFilename.equals("")) qM.put("aFilename", asset.aFilename)
    
       val count = db.getCollection("fs.files").count(qM)
       result = count.toString()
    }
    mongoClient.close()
    result
  }
  //
   // remove document based on mongodb id  
  def removeDocument (id:String): String = {
    val statusCode = new StatusCode(200)
    if (mongoOk) {
      logger.info("removeDocument: started")
      var gridDoc = new com.mongodb.gridfs.GridFS(db)
      val query = new BasicDBObject("id", id)
      val doc:com.mongodb.gridfs.GridFSDBFile = gridDoc.findOne(query)
      if (doc == null) {
        statusCode.status = 404
        statusCode.error = "Document not found"
        statusCode.message = "Could not delete document " + id
      } else {
        val wr = gridDoc.remove(query);
      }
    } else {
      statusCode.status = 401
      statusCode.error = "MongoDB is down"
      statusCode.message = "contact system administrator"
    }
    mongoClient.close()
   statusCode.toString()
  }
  //
  // remove documents matching criteria 
  def removeDocumentsMatching (asset:AssetFilter): Integer = {
    var nRemoved = 0
    if (mongoOk) {
      logger.info("removeDocumentsMatching: started")
       var gridDoc = new com.mongodb.gridfs.GridFS(db)
       //val qM:java.util.Map[String, String] = new java.util.HashMap()
       val qM = new BasicDBObject()
       if (!asset.id.equals("")) qM.put("id", asset.id)
       if (!asset.aCompanyName.equals("")) qM.put("aCompanyName", asset.aCompanyName)
       if (!asset.aServiceName.equals("")) qM.put("aServiceName", asset.aServiceName)
       if (!asset.aUsername.equals("")) qM.put("aUsername", asset.aUsername)
       if (!asset.aDocumentType.equals("")) qM.put("aDocumentType", asset.aDocumentType)
       if (!asset.aFilename.equals("")) qM.put("aFilename", asset.aFilename)
       if (!asset.aExtension.equals("")) qM.put("aExtension", asset.aExtension)
       val docs:java.util.List[com.mongodb.gridfs.GridFSDBFile] = gridDoc.find(qM)
       val it:java.util.Iterator[com.mongodb.gridfs.GridFSDBFile] = docs.iterator()
       val from = asset.aFrom
       val to = asset.aTo
       //logger.debug("docs Size:" + docs.size())
       while (it.hasNext()) {
         breakable {
           val doc:com.mongodb.gridfs.GridFSDBFile = it.next()
           val mongoDate = new Date(doc.get("uploadDate").toString())
           val df:DateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
           val asISO = df.format(mongoDate)
           //println ("uploadDate:" + asISO + " from:" + from + " to:" + to)
           if (asISO < from || asISO > to) break
           // remove document
           val rm = new BasicDBObject()
           
           rm.put("id", doc.get("id"))
           gridDoc.remove(rm)
           nRemoved = nRemoved + 1
         }
       }
    }
    mongoClient.close()
    nRemoved
  }
  // get ZIP Array Photos from a list of file names
  def getZIPArrayPhotos (fileNames:java.util.Set[String]): String = {
    
    var nowB = Calendar.getInstance()
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    var strDate = sdf.format(nowB.getTime())
    
    var result = filePath + "visitorPhotosArray.zip"
    if (mongoOk) {
      logger.info("getZIPArrayPhotos: started")
      var gridDoc = new com.mongodb.gridfs.GridFS(db)
      val files:MutableList[String] = MutableList()
      val filesStream:MutableList[InputStream] = MutableList()
      //println("fileNames:" + fileNames.size() + " array:" + fileNames.toString())
      val it = fileNames.iterator()
      while (it.hasNext()) {
        val qM = new BasicDBObject()
        qM.put("aFilename", it.next().trim())
        val doc:com.mongodb.gridfs.GridFSDBFile = gridDoc.findOne(qM)
        if (doc != null) {
          val fileName = doc.get("aFilename").toString()
          val tempFile = filePath + fileName
          //println(doc.toString())
          files += tempFile
          // instead of writing to file system
          filesStream += doc.getInputStream()
        }
      }
       
      ZipperStream(result, files, filesStream, false)
      logger.debug("getZIPArrayPhotos -- got latest successfully : ")
      val nowA= Calendar.getInstance()
      strDate = sdf.format(nowA.getTime())
      logger.info("\n\nMongoDB finished at:" + strDate + " processed:" + fileNames.size() + " returned:" + files.size + " elapsed:" + "%.2f".format((nowA.getTimeInMillis() - nowB.getTimeInMillis)/1000.0))

    } else {
      result = ""
    }
    mongoClient.close()
    result
  }
  //
  val Buffer = 2 * 1024

  def Zipper(out: String, files: Iterable[String], retainPathInfo: Boolean = true) = {
    var data = new Array[Byte](Buffer)
    val zip = new ZipOutputStream(new FileOutputStream(out))
    var path = ""
    files.foreach { name =>
      breakable {
        if (!retainPathInfo) path = name.splitAt(name.lastIndexOf(File.separatorChar) + 1)._2
        else                 path = name
        // correction for windows machines
        val index = name.lastIndexOf("/")
        val sName = name.substring(index + 1)
        try {
          zip.putNextEntry(new ZipEntry(sName))
        } catch {
          case ezip: java.util.zip.ZipException => {
            //println ("duplicated entry:" + sName)
            break
          }
          case e: Exception => break 
        }
        //println("name=" + name + " sName=" + sName + " separator=" + File.separator + " os:" + System.getProperty("os.name"))
        //zip.putNextEntry(new ZipEntry(path))
        val in = new BufferedInputStream(new FileInputStream(name), Buffer)
        var b = in.read(data, 0, Buffer)
        while (b != -1) {
          zip.write(data, 0, b)
          b = in.read(data, 0, Buffer)
        }
        in.close()
        zip.closeEntry()
        val fileTemp = new File(name)
        fileTemp.delete()
      }
      
    }
    zip.close()
  }
  //
  def ZipperStream(out: String, files: MutableList[String], filesStream:MutableList[InputStream], retainPathInfo: Boolean = true) = {
    var data = new Array[Byte](Buffer)
    val zip = new ZipOutputStream(new FileOutputStream(out))
    var path = ""
    var i = 0
    for (i <- 0 to filesStream.size - 1) {
      breakable {
        val name = files(i)
        if (!retainPathInfo) path = name.splitAt(name.lastIndexOf(File.separatorChar) + 1)._2
        else                 path = name
        // correction for windows machines
        val index = name.lastIndexOf("/")
        val sName = name.substring(index + 1)
        try {
          zip.putNextEntry(new ZipEntry(sName))
        } catch {
          case ezip: java.util.zip.ZipException => {
            //println ("duplicated entry:" + sName)
            break
          }
          case e: Exception => break 
        }
      
        //zip.putNextEntry(new ZipEntry(path))
        val in = new BufferedInputStream(filesStream(i), Buffer)
        var b = in.read(data, 0, Buffer)
        while (b != -1) {
          zip.write(data, 0, b)
          b = in.read(data, 0, Buffer)
        }
        in.close()
        zip.closeEntry()
      }
      
    }
      
    zip.close()
  }
  //
   //
  def ZipperStreamLinkedHashSet(out: String, files:java.util.LinkedList[String], filesDoc: java.util.LinkedList[com.mongodb.gridfs.GridFSDBFile], retainPathInfo: Boolean = true) = {
    var data = new Array[Byte](Buffer)
    val zip = new ZipOutputStream(new FileOutputStream(out))
    var path = ""
    val itFiles = files.iterator
    val itFilesDoc = filesDoc.iterator
    var i = 0
    while (itFiles.hasNext) {
      breakable {
        val name = itFiles.next()
        val doc = itFilesDoc.next()
        if (!retainPathInfo) path = name.splitAt(name.lastIndexOf(File.separatorChar) + 1)._2
        else                 path = name
        // correction for windows machines
        val index = name.lastIndexOf("/")
        val sName = name.substring(index + 1)
        try {
          zip.putNextEntry(new ZipEntry(sName))
          i += 1
        } catch {
          case ezip: java.util.zip.ZipException => {
            //println (" ziiping duplicated entry:" + sName)
            break
          }
          case e: Exception => break 
        }
        //println("zipped name=" + sName)
        //println("name=" + name + " sName=" + sName + " separator=" + File.separator + " os:" + System.getProperty("os.name"))
        //zip.putNextEntry(new ZipEntry(path))
        val in = new BufferedInputStream(doc.getInputStream, Buffer)
        var b = in.read(data, 0, Buffer)
        while (b != -1) {
          zip.write(data, 0, b)
          b = in.read(data, 0, Buffer)
        }
        in.close()
        zip.closeEntry()
      }
      
    }
    //println("after Zip size:" + i)  
    zip.close()
  }
  //
  def mongoDBIsRunning():Boolean = {
    mongoOk
  }
   // store document in mongodb and return id
  def uploadAndReturnId (asset:Asset, filenamePath:String, filename:String, contentType:String): StatusCode = {
    val statusCode = new StatusCode(200)
    var documentId = ""
    if (mongoOk) {
      logger.info("storeAndReturnId: started")
      var gridDoc = new com.mongodb.gridfs.GridFS(db)
      logger.info("storeDocument - filename on: " + filenamePath + filename)
      if (new File(filenamePath + filename).exists()) {
        val fi:FileInputStream = new FileInputStream(filenamePath + filename) 
        val in:com.mongodb.gridfs.GridFSInputFile = gridDoc.createFile(fi)
        in.put("id", asset.id)
        in.put("aCompanyName", asset.aCompanyName)
        in.put("aServiceName", asset.aServiceName)
        in.put("aUsername", asset.aUsername)
        in.put("aDocumentType", asset.aDocumentType)
        in.put("aContentType", contentType)
        var fn = ""
        if (asset.aFilename.equals("")){
          in.put("aFilename", filename)
          fn = filename
        } 
        else {
          in.put("aFilename", asset.aFilename)
          fn = asset.aFilename
        }                           
        in.put("aExtension", asset.aExtension)
        in.put("aPictureWidth", asset.aPictureWidth)
        in.put("aPictureHeight", asset.aPictureHeight)
        in.put("aStatus", asset.aStatus)
        // create meta data
        val md = new BasicDBObject()
        md.put("aDisplayName", fn)
        md.put("aContentType", contentType)
        in.setMetaData(md)
        //
        in.save()
        documentId = in.getId().toString
        statusCode.message = documentId
        logger.debug("storeDocument -- stored document successfully : " + fn)
       
      } else {
        statusCode.status = 404
        statusCode.error = "storeDocument - File not found while uploading - file path: " + filenamePath + filename
        statusCode.message = "Check upload process"
        logger.error("storeDocument -- could not find file:" + filenamePath + filename + " check upload process (404)")
      }
       mongoClient.close()
    } else {
      statusCode.status = 401
      statusCode.error = "mongoDB is down - contact system administrator"
      statusCode.message = "Verify if mongoDB is running"
      logger.error("storeDocument -- store document failed: mongoDB is down (401)")
    }
    
    statusCode
  }
  //
  // get document by mongo db id
  def getDocumentByMongoId (_id:String): Array[String] = {
     
     var result = Array("", "")
     if (mongoOk) {
       logger.info("get Document by Mongo Id: started")
       var gridDoc = new com.mongodb.gridfs.GridFS(db)
       val query = new BasicDBObject()
       
       try {
         query.put("_id", new org.bson.types.ObjectId(_id.trim()))
         val doc:com.mongodb.gridfs.GridFSDBFile = gridDoc.findOne(query);
         
         //val filename = doc.get("aCompanyName").toString() + doc.get("aServiceName").toString() + doc.get("aUsername").toString()
         val filename = _id
         val extension = doc.get("aExtension").asInstanceOf[String].replaceAll("\\.", "")
         //logger.debug("aExtension=" + doc.get("aExtension") + " extension: " + extension) 
         val f = filePath + filename + "." + extension
         
         //logger.debug("f= " + f)
         doc.writeTo(filePath + filename + "." + extension)
         result(0) = filePath + filename + "." + extension
         result(1) = extension
         logger.debug("get Document by Mongo Id successful - filename: " + filename + " extension: " + extension)
       } catch {
         case ex: MongoException => {
         logger.error(ex.getMessage())
         }
         case ex: Exception => {
         logger.error(ex.getMessage())
         }
       }
       mongoClient.close()
     }
    result
  } 
  // remove document by mongodb id  
  def removeDocumentByMongoId (_id:String): String = {
    val statusCode = new StatusCode(200)
    if (mongoOk) {
      var gridDoc = new com.mongodb.gridfs.GridFS(db)
      val query = new BasicDBObject("_id", new org.bson.types.ObjectId(_id))
      val doc:com.mongodb.gridfs.GridFSDBFile = gridDoc.findOne(query)
      if (doc == null) {
        statusCode.status = 404
        statusCode.error = "Document not found"
        statusCode.message = "Could not delete document " + _id
      } else {
        val wr = gridDoc.remove(query);
        logger.debug("removeDocumentByMongoId - succesful - removed document with mongo id: " + _id)
      }
    } else {
      statusCode.status = 401
      statusCode.error = "MongoDB is down"
      statusCode.message = "contact system administrator"
    }
    mongoClient.close()
   statusCode.toString()
  }
  //
  // store document with _id in mongodb
  def storeDocumentWith_id (_id:String, asset:Asset, filenamePath:String, filename:String, contentType:String): StatusCode = {
    val statusCode = new StatusCode(200)
    if (mongoOk) {
      logger.info("storeDocument with id: started")
      var gridDoc = new com.mongodb.gridfs.GridFS(db)
      val file  = new File(filenamePath + filename) 
      if (file.exists()) {
        logger.info("storeDocument - filename on: " + file + " length: " + file.length())
        val fi:FileInputStream = new FileInputStream(file) 
        val in:com.mongodb.gridfs.GridFSInputFile = gridDoc.createFile(fi)
        in.put("_id", new org.bson.types.ObjectId(_id))    // mongodb d
        in.put("id", asset.id)                             // custom id
        in.put("aCompanyName", asset.aCompanyName)
        in.put("aServiceName", asset.aServiceName)
        in.put("aUsername", asset.aUsername)
        in.put("aDocumentType", asset.aDocumentType)
        in.put("aContentType", contentType)
        var fn = ""
        if (asset.aFilename.equals("")){
          in.put("aFilename", filename)
          fn = filename
        } 
        else {
          in.put("aFilename", asset.aFilename)
          fn = asset.aFilename
        }                           
        in.put("aExtension", asset.aExtension)
        in.put("aPictureWidth", asset.aPictureWidth)
        in.put("aPictureHeight", asset.aPictureHeight)
        in.put("aStatus", asset.aStatus)
        val md = new BasicDBObject()
        md.put("aDisplayName", filename)
        md.put("aContentType", contentType)
        in.setMetaData(md)
        //
        in.save()
        
        statusCode.message = in.getId().toString
        logger.debug("storeDocument -- stored document successfully : " + fn)
        mongoClient.close()
      } else {
        statusCode.status = 404
        statusCode.error = "storeDocument - File not found while storing - file path: " + filenamePath + filename
        statusCode.message = "Check upload process"
        logger.error("storeDocument -- could not find file:" + filenamePath + filename + " check upload process (404)")
      }
      
    } else {
      statusCode.status = 401
      statusCode.error = "mongoDB is down - contact system administrator"
      statusCode.message = "Verify if mongoDB is running"
      logger.error("storeDocument -- store document failed: mongoDB is down (401)")
    }
    
    statusCode
  }
   // retrieve input stream by mongodb id
  def retrieveInputStreamByMongoId (_id:String): java.io.InputStream = {
    
    var result:java.io.InputStream = null
    if (mongoOk) {
      logger.info("retrieveInputStreamByMongoId: started")
        // see if object is in the play cache
        var gridDoc = new com.mongodb.gridfs.GridFS(db)
        val query = new BasicDBObject("_id", new org.bson.types.ObjectId(_id))
        try {
           val docs:java.util.List[com.mongodb.gridfs.GridFSDBFile] = gridDoc.find(query);
           var initialFile:com.mongodb.gridfs.GridFSDBFile = docs.get(0);
           var latestIndex = 0
           //println("doc:" +  docs.get(0).toString() + " aqui:" + docs.get(0).get("uploadDate").toString() + " size:" + docs.size());
           if (docs.size() > 1) {
             // pick the latest one
             for (i <- 1 to docs.size() - 1) {
               val oneFile:com.mongodb.gridfs.GridFSDBFile = docs.get(i);
               if (oneFile.get("uploadDate").toString() > initialFile.get("uploadDate").toString()) latestIndex = i
               initialFile = oneFile
               //println("i:" + i + " id:" + oneFile.get("aCompanyName").toString() + " aFilename:" + oneFile.get("aFilename").toString())
             }
           }
           val doc:com.mongodb.gridfs.GridFSDBFile = docs.get(latestIndex)
           result = doc.getInputStream
           logger.debug("retrieveInoutStreamByFilename -- retrieved inout stream successfully : ")
        } catch {
            case ex: MongoException => {
              logger.error(ex.getMessage())
            }
            case ex: Exception => {
              logger.error(ex.getMessage())
            }
        }
 
    } 
    mongoClient.close()
    result
  } 
  // query document by mongo id  
  def queryDocumentByMongoId (_id:String): String = {
    var result = ""
    if (mongoOk) {
      logger.info("get Document by Mongo Id: started")
      var gridDoc = new com.mongodb.gridfs.GridFS(db)
      val query = new BasicDBObject("_id", new org.bson.types.ObjectId(_id))
      val doc = gridDoc.findOne(query)
      if (doc != null) result = doc.toString()
    }
    mongoClient.close()
    result
  }
  // update file name by mongo id  
  def updateMetaDataByMongoId (_id:String, displayName:String, contentType:String): StatusCode = {
    var sC = new StatusCode(200)
    if (mongoOk) {
      logger.info("update Filename by Mongo Id: started")
      var gridDoc = new com.mongodb.gridfs.GridFS(db)
      val query = new BasicDBObject("_id", new org.bson.types.ObjectId(_id))    
      val doc = gridDoc.findOne(query)
      if (doc != null) {
        val md = new BasicDBObject()
        md.put("aDisplayName", displayName)
        md.put("aContentType", contentType)
        //logger.debug(doc.getMetaData.toString())
        doc.setMetaData(md)
        doc.save()
        sC.message = "updated aDisplayName to: " + displayName + " contentType: " + contentType
      } else {
        sC.status = 404
        sC.message = "Verify document in Mongo DB"
        sC.error = "Document not found"
      }
    } else {
      sC.status = 400
      sC.message = "Could not update filename"
      sC.error = "Mongo Db is not running - contact administrator"
    }
    mongoClient.close()
    sC
  }
  //
  // get display meta name by mongodb id  
  def getDisplayNameByMongoId (_id:String): String = {
    var result = ""
    if (mongoOk) {
      logger.info("get Document by Mongo Id: started")
      var gridDoc = new com.mongodb.gridfs.GridFS(db)
      val query = new BasicDBObject("_id", new org.bson.types.ObjectId(_id))
      val doc = gridDoc.findOne(query)
      val obj = doc.getMetaData
      val displayName = obj.get("aDisplayName")
      if (displayName != null) result = displayName.toString()
    }
    mongoClient.close()
    result
  }

  // get filename by mongodb id
  def getFileNameMongoId (name:String): String = {
    
    var result = ""
    if (mongoOk) {
      logger.info("get filename mongo db id: started")
        // see if object is in the play cache
        var gridDoc = new com.mongodb.gridfs.GridFS(db)
        val query = new BasicDBObject("aFilename", name)
        try {
           val docs:java.util.List[com.mongodb.gridfs.GridFSDBFile] = gridDoc.find(query);
           var initialFile:com.mongodb.gridfs.GridFSDBFile = docs.get(0);
           var latestIndex = 0
           //println("doc:" +  docs.get(0).toString() + " aqui:" + docs.get(0).get("uploadDate").toString() + " size:" + docs.size());
           if (docs.size() > 1) {
             // pick the latest one
             for (i <- 1 to docs.size() - 1) {
               val oneFile:com.mongodb.gridfs.GridFSDBFile = docs.get(i);
               if (oneFile.get("uploadDate").toString() > initialFile.get("uploadDate").toString()) {
                 latestIndex = i
               }
               initialFile = oneFile
               //println("i:" + i + " id:" + oneFile.get("aCompanyName").toString() + " aFilename:" + oneFile.get("aFilename").toString())
             }
           }
           val doc:com.mongodb.gridfs.GridFSDBFile = docs.get(latestIndex)
           result = doc.get("_id").toString()
           logger.debug("get filename mongo db id successful : " + result)
        } catch {
            case ex: MongoException => {
              logger.error(ex.getMessage())
            }
            case ex: Exception => {
              logger.error(ex.getMessage())
            }
        }
 
    } 
    mongoClient.close()
    result
  } 
}