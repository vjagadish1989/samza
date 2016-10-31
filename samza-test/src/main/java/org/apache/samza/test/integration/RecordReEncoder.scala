package org.apache.samza.test.integration

import java.io.ByteArrayOutputStream

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{BinaryDecoder, BinaryEncoder, DecoderFactory}
import org.apache.samza.metrics.Counter
import org.apache.samza.util.Logging

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Re-encode instances of GenericRecord to the schema provided at instance creation, should they not already be based
  * upon that reference schema.  This class assumes that any schema which is not equal to the class's schema field is
  * an older version of it.  This means this class' behavior is undefined when passing in a schema which is actually
  * newer than the reference schema.
  *
  * To avoid an expensive equals operation on each schema, we key the cache on the schema's hashcode.  Later versions
  * of Avro cheapen the cost of the equals method but with the current 1.4 we're using, this was a performance killer.
  * Caveat utilitor.
  *
  * @param referenceSchema Reader schema against which all other schemas are compared
  * @param counter Optional counter to increment each time a record is re-encoded
  */
class RecordReEncoder(referenceSchema:Schema, counter:Option[Counter]) extends Logging {
  var resolvingReaders = mutable.Map[Int, GenericDatumReader[GenericRecord]]()
  var decoder:BinaryDecoder = null
  val baos = new ByteArrayOutputStream()
  val origHash = referenceSchema.hashCode()

  private def getReader(key:Int, s:Schema) = {
    if(!resolvingReaders.containsKey(key)) {
      info(toString + ": Adding new reader for schema: " + s)
    }
    resolvingReaders.getOrElseUpdate(key, new GenericDatumReader[GenericRecord](s, referenceSchema))
  }

  /**
    * Take a GenericRecord and, if its schema matches the original schema provided to the instance,
    * return it unchanged.  If, however, the schema is different, re-encoded the record against
    * the reference schema, returning this new intance in its place.
    *
    * @param r record to potentially re-encode
    * @return possible re-encoded record or the original
    */
  def reEncode(r:GenericRecord):GenericRecord = {
    val hash = r.getSchema.hashCode()
    try {
      if (hash.equals(origHash)) {
        r
      } else {
        counter.foreach(_.inc)
        baos.reset()
        val be = new BinaryEncoder(baos)
        val gdw = new GenericDatumWriter[GenericRecord](r.getSchema)
        gdw.write(r, be)

        decoder = DecoderFactory.defaultFactory().createBinaryDecoder(baos.toByteArray, decoder)
        val evolvedRecord = new GenericData.Record(referenceSchema)
        val gdr = getReader(hash, r.getSchema)
        gdr.read(evolvedRecord, decoder)
        evolvedRecord
      }
    } catch {
      case e: Exception => warn("Re-throwing exception while re-encoding " + r + "(schema = " + r.getSchema + "), against " + referenceSchema, e)
        throw e
    }
  }

  override def toString: String = "RecordReEncoder for schema "  + referenceSchema.getFullName  + " with " + resolvingReaders.size + " cached re-encoders"
}
