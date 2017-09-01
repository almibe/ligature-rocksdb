/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.almibe.ligature.store

import com.orientechnologies.orient.core.db.ODatabasePool
import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.db.document.ODatabaseDocument
import com.orientechnologies.orient.core.record.ODirection
import com.orientechnologies.orient.core.record.OEdge
import com.orientechnologies.orient.core.record.OVertex
import org.almibe.ligature.*
import java.util.stream.Stream

class OrientDBLigatureStore(private val databasePool: ODatabasePool): Model {
    private val IRI_CLASS = "IRI"
    private val IRI_EDGE_CLASS = "IRIEdge"
    private val TYPED_LITERAL_CLASS = "TypedLiteral"
    private val LANG_LITERAL_CLASS = "LangLiteral"
    private val BLANK_NODE_CLASS = "BlankNode"

    init {
        val db = databasePool.acquire()
        if (db.getClass(IRI_CLASS) == null) db.createVertexClass(IRI_CLASS)
        if (db.getClass(IRI_EDGE_CLASS) == null) db.createEdgeClass(IRI_EDGE_CLASS)
        if (db.getClass(BLANK_NODE_CLASS) == null) db.createVertexClass(BLANK_NODE_CLASS)
        if (db.getClass(TYPED_LITERAL_CLASS) == null) db.createVertexClass(TYPED_LITERAL_CLASS)
        if (db.getClass(LANG_LITERAL_CLASS) == null) db.createVertexClass(LANG_LITERAL_CLASS)
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
                val edge = subjectVertex.addEdge(objectVertex, IRI_EDGE_CLASS)
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
        val vertex = db.newVertex(IRI_CLASS)
        vertex.setProperty("value", iri.value)
        vertex.save<OVertex>()
        return vertex
    }

    private fun createBlankNode(blankNode: BlankNode, db: ODatabaseDocument): OVertex {
        val vertex = db.newVertex(BLANK_NODE_CLASS)
        vertex.setProperty("label", blankNode.label)
        vertex.save<OVertex>()
        return vertex
    }

    private fun createLangLiteral(langLiteral: LangLiteral, db: ODatabaseDocument): OVertex {
        val vertex = db.newVertex(LANG_LITERAL_CLASS)
        vertex.setProperty("value", langLiteral.value)
        vertex.setProperty("langTag", langLiteral.langTag)
        vertex.save<OVertex>()
        return vertex
    }

    private fun createTypedLiteral(typedLiteral: TypedLiteral, db: ODatabaseDocument): OVertex {
        val vertex = db.newVertex(TYPED_LITERAL_CLASS)
        vertex.setProperty("value", typedLiteral.value)
        vertex.setProperty("type", typedLiteral.datatypeIRI.value)
        vertex.save<OVertex>()
        return vertex
    }

    fun getIRIs(): Set<IRI> {
        val db = databasePool.acquire()
        val iris = HashSet<IRI>()
        var resultSet = db.browseClass(IRI_CLASS)
        while (resultSet.hasNext()) {
            val result = resultSet.next()
            val value = result.getProperty<String>("value")
            iris.add(IRI(value))
        }
        resultSet = db.browseClass(IRI_EDGE_CLASS)
        while (resultSet.hasNext()) {
            val result = resultSet.next()
            val value = result.getProperty<String>("value")
            iris.add(IRI(value))
        }
        resultSet = db.browseClass(TYPED_LITERAL_CLASS)
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
        val vertexStream = subjectToVertex(subject, db)
        vertexStream.forEach { subjectVertex ->
            subjectVertex.getEdges(ODirection.OUT).forEach { edge ->
                val predicate = IRI(edge.getProperty("value"))
                val `object` = edge.to
                val resultObject = if (`object`.schemaType.get().name == IRI_CLASS) {
                    IRI(`object`.getProperty("value"))
                } else if (`object`.schemaType.get().name == TYPED_LITERAL_CLASS) {
                    TypedLiteral(`object`.getProperty("value"), IRI(`object`.getProperty("type")))
                } else if (`object`.schemaType.get().name == LANG_LITERAL_CLASS) {
                    LangLiteral(`object`.getProperty("value"), `object`.getProperty("langTag"))
                } else if (`object`.schemaType.get().name == BLANK_NODE_CLASS) {
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

    override fun removeStatement(subject: Subject, predicate: Predicate, `object`: Object) {
        val db = databasePool.acquire()
        db.use {
            val subjectVertex = subjectToVertex(subject, db).findFirst().get()
            val predicateEdge = subjectVertex.getEdges(ODirection.OUT).filter { edge ->
                predicate is IRI && predicate.value == edge.getProperty<String>("value") && vertexAndObjectEqual(edge.to, `object`)
            }

        }
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun removeSubject(subject: Subject) {
        val db = databasePool.acquire()
        db.use {

        }
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    private fun subjectToVertex(subject: Subject, db: ODatabaseSession): Stream<OVertex> {
        return when (subject) {
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
    }

    private fun vertexAndObjectEqual(vertex: OVertex, `object`: Object): Boolean {
        val vertexType = vertex.schemaType.toString()
        return when (`object`) {
            is IRI -> vertexType == IRI_CLASS && `object`.value == vertex.getProperty<String>("value")
            is BlankNode -> vertexType == BLANK_NODE_CLASS && `object`.label == vertex.getProperty<String>("label")
            is LangLiteral -> vertexType == LANG_LITERAL_CLASS && `object`.langTag == vertex.getProperty<String>("langTag")
                && `object`.value == vertex.getProperty<String>("value")
            is TypedLiteral -> vertexType == TYPED_LITERAL_CLASS && `object`.datatypeIRI.value == vertex.getProperty<String>("type")
                && `object`.value == vertex.getProperty<String>("value")
            else -> throw RuntimeException("Unexpected object type $`object`")
        }
    }
}
