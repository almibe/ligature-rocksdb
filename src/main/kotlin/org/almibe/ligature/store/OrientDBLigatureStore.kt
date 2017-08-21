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
        db.createVertexClass("IRI")
        db.createEdgeClass("IRIEdge")
        db.createVertexClass("BlankNode")
        db.createVertexClass("TypedLiteral")
        db.createVertexClass("LangLiteral")
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
        TODO("finish")
    }

    private fun createTypedLiteral(typedLiteral: TypedLiteral, db: ODatabaseDocument): OVertex {
        TODO("finish")
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
        if (subject is IRI) {
            val rs = db.query("SELECT FROM IRI WHERE value = ?", subject.value)
            rs.vertexStream().forEach { subject ->
                subject.getEdges(ODirection.OUT).forEach { edge ->
                    val predicate = IRI(edge.getProperty("value"))
                    val `object` = IRI(edge.to.getProperty("value")) //TODO object might not be an IRI
                    results.add(Pair(predicate, `object`))
                }
            }
        }
        db.close()
        return results
    }
}
