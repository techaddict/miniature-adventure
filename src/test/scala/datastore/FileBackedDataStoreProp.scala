package datastore

import org.scalacheck.Prop.forAll
import org.scalacheck.{Gen, Properties}

object FileBackedDataStoreProp extends Properties("String") {

  val dataStore: FileBackedDataStore = new FileBackedDataStore(Config())
  dataStore.init()

  property("put and then get") = forAll { (k: String, v: String) =>
    if(dataStore.put(k, v)) {
      dataStore.get(k).isDefined && dataStore.get(k).contains(v)
    } else {
      true
    }
  }
}
