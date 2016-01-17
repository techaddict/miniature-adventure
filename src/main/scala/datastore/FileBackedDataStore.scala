package datastore

import java.io.{RandomAccessFile, File}

import scala.collection.mutable
import scala.util.Random
import scala.util.control.NonFatal

class FileBackedDataStore extends DataStore {
  private[datastore] var underlyingFile: RandomAccessFile = null
  private[datastore] var locationInfo: mutable.Map[String, Long] = null

  //remove this after init handles reading data from older file
  def init(): Boolean = init("data" + File.separator + Random.alphanumeric.take(10).mkString(""))

  private[datastore] def init(filename: String): Boolean = {
    try {
      val dir = new File("data")
      if(!dir.exists()) {
        dir.mkdir()
        locationInfo = mutable.Map[String, Long]()
        underlyingFile = new RandomAccessFile(filename, "rwd")
      } else {
        locationInfo = mutable.Map[String, Long]()
        underlyingFile = new RandomAccessFile(filename, "rwd")
        //read and prepare cache.
      }
      true
    } catch {
      case NonFatal(e) =>
        false
    }
  }

  def close(): Boolean = {
    try {
      underlyingFile.close()
      true
    } catch {
      case NonFatal(e) =>
        false
    }
  }

  private[datastore] def clean() = {
    val dir = new File("data")
    dir.listFiles().foreach {_.delete()}
  }

  def iterator(): Iterator[(String, String)] = {
    val keys = locationInfo.keysIterator
    keys.map{ key =>
      underlyingFile.seek(locationInfo(key))
      (key, underlyingFile.readUTF())
    }
  }

  def get(key: String): Option[String] = {
    if(locationInfo.contains(key)) {
      underlyingFile.seek(locationInfo(key))
      Some(underlyingFile.readUTF())
    } else {
      None
    }
  }

  def put(key: String, value: String): Boolean = {
    if(!locationInfo.contains(key)) {
      underlyingFile.seek(underlyingFile.length())
      underlyingFile.writeUTF(key)
      locationInfo.put(key, underlyingFile.getFilePointer)
      underlyingFile.writeUTF(value)
      true
    } else {
      false
    }
  }
}
