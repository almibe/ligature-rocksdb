/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.almibe.ligature.store

import com.orientechnologies.orient.core.db.ODatabasePool
import com.orientechnologies.orient.core.db.document.ODatabaseDocument
import com.orientechnologies.orient.core.record.ODirection
import com.orientechnologies.orient.core.record.OEdge
import com.orientechnologies.orient.core.record.OVertex
import org.almibe.ligature.*
import java.util.stream.Stream

class OrientDBLigatureStore(private val databasePool: ODatabasePool): Model {

    init {
        val db = databasePool.acquire()
        if (db.getClass("IRI") == null) db.createVertexClass("IRI")
        if (db.getClass("IRIEdge") == null) db.createEdgeClass("IRIEdge")
        if (db.getClass("BlankNode") == null) db.createVertexClass("BlankNode")
        if (db.getClass("TypedLiteral") == null) db.createVertexClass("TypedLiteral")
        if (db.getClass("LangLiteral") == null) db.createVertexClass("LangLiteral")
        db.close()
    }

    override fun addModel(model: ReadOnlyModel) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun addStatement(subject: Subject, predicate: Predicate, `object`: Object) {
        val db = databasePool.acquire()
        try {
            db.begin()

            val subjectVertex = when (subject) {
                is IRI -> createIRI(subject, db)
                is BlankNode -> createBlankNode(subject, db)
                else -> throw RuntimeException("Unexpected subject type $subject")
            }

            val objectVertex = when (`object`) {
                is IRI -> createIRI(`object`, db)
                is BlankNode -> createBlankNode(`object`, db)
                is LangLiteral -> createLangLiteral(`object`, db)
                is TypedLiteral -> createTypedLiteral(`object`, db)
                else -> throw RuntimeException("Unexpected object type $`object`")
            }

            if (predicate is IRI) {
                val edge = subjectVertex.addEdge(objectVertex, "IRIEdge")
                edge.setProperty("value",predicate.value)
                edge.save<OEdge>()
            } else {
                throw RuntimeException("Unexpected predicate type $predicate")
            }

            db.commit()
        } catch (ex: Exception) {
            db.rollback()
        }
        db.close()
    }

    private fun createIRI(iri: IRI, db: ODatabaseDocument): OVertex {
        val vertex = db.newVertex("IRI")
        vertex.setProperty("value", iri.value)
        vertex.save<OVertex>()
        return vertex
    }

    private fun createBlankNode(blankNode: BlankNode, db: ODatabaseDocument): OVertex {
        val vertex = db.newVertex("BlankNode")
        vertex.setProperty("label", blankNode.label)
        vertex.save<OVertex>()
        return vertex
    }

    private fun createLangLiteral(langLiteral: LangLiteral, db: ODatabaseDocument): OVertex {
        val vertex = db.newVertex("LangLiteral")
        vertex.setProperty("value", langLiteral.value)
        vertex.setProperty("langTag", langLiteral.langTag)
        vertex.save<OVertex>()
        return vertex
    }

    private fun createTypedLiteral(typedLiteral: TypedLiteral, db: ODatabaseDocument): OVertex {
        val vertex = db.newVertex("TypedLiteral")
        vertex.setProperty("value", typedLiteral.value)
        vertex.setProperty("type", typedLiteral.datatypeIRI.value)
        vertex.save<OVertex>()
        return vertex
    }

    fun getIRIs(): Set<IRI> {
        val db = databasePool.acquire()
        val iris = HashSet<IRI>()
        var resultSet = db.browseClass("IRI")
        while (resultSet.hasNext()) {
            val result = resultSet.next()
            val value = result.getProperty<String>("value")
            iris.add(IRI(value))
        }
        resultSet = db.browseClass("IRIEdge")
        while (resultSet.hasNext()) {
            val result = resultSet.next()
            val value = result.getProperty<String>("value")
            iris.add(IRI(value))
        }
        resultSet = db.browseClass("TypedLiteral")
        while (resultSet.hasNext()) {
            val result = resultSet.next()
            val value = result.getProperty<String>("type")
            iris.add(IRI(value))
        }
        db.close()
        return iris
    }

    override fun getSubjects(): Stream<Subject> {
        val db = databasePool.acquire()
        val vertexStream = db.query("SELECT FROM IRI").vertexStream()
        vertexStream.onClose { db.close() }
        return vertexStream.map { vertex ->
            IRI(vertex.getProperty("value"))
        }
    }

    override fun statementsFor(subject: Subject): Set<Pair<Predicate, Object>> {
        val results = HashSet<Pair<Predicate, Object>>()
        val db = databasePool.acquire()
        val vertexStream = when (subject) {
            is IRI -> {
                val rs = db.query("SELECT FROM IRI WHERE value = ?", subject.value)
                rs.vertexStream()
            }
            is BlankNode -> {
                val rs = db.query("SELECT FROM BlankNode WHERE label = ?", subject.label)
                rs.vertexStream()
            }
            else -> throw RuntimeException("Unexpected subject $subject")
        }
        vertexStream.forEach { subjectVertex ->
            subjectVertex.getEdges(ODirection.OUT).forEach { edge ->
                val predicate = IRI(edge.getProperty("value"))
                val `object` = edge.to
                val resultObject = if (`object`.schemaType.get().name == "IRI") {
                    IRI(`object`.getProperty("value"))
                } else if (`object`.schemaType.get().name == "TypedLiteral") {
                    TypedLiteral(`object`.getProperty("value"), IRI(`object`.getProperty("type")))
                } else if (`object`.schemaType.get().name == "LangLiteral") {
                    LangLiteral(`object`.getProperty("value"), `object`.getProperty("langTag"))
                } else if (`object`.schemaType.get().name == "BlankNode") {
                    BlankNode(`object`.getProperty("label"))
                } else {
                    throw RuntimeException("Unexpected object $`object`")
                }
                results.add(Pair(predicate, resultObject))
            }
        }
        db.close()
        return results
    }
}
