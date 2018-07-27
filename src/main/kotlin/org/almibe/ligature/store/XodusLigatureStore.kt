/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.almibe.ligature.store

import jetbrains.exodus.entitystore.PersistentEntityStore
import org.almibe.ligature.*
import java.util.stream.Collectors
import java.util.stream.Stream

class XodusLigatureStore(private val entityStore: PersistentEntityStore): Model {
    private val iriClass = "ligature.IRI"
    private val iriEdgeClass = "ligature.IRIEdge"
    private val typedLiteralClass = "ligature.TypedLiteral"
    private val langLiteralClass = "ligature.LangLiteral"
    private val blankNodeClass = "ligature.BlankNode"

    override fun addModel(model: ReadOnlyModel) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun addStatement(subject: Subject, predicate: Predicate, `object`: Object) {
        entityStore.executeInTransaction {
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
                    val edge = subjectVertex.addEdge(objectVertex, iriEdgeClass)
                    edge.setProperty("value",predicate.value)
                    edge.save<OEdge>()
                } else {
                    throw RuntimeException("Unexpected predicate type $predicate")
                }

                db.commit()
            } catch (ex: Exception) {
                db.rollback()
            }
        }
    }

    override fun addSubject(subject: Subject) {
        val db = databasePool.acquire()
        try {
            db.begin()
            when (subject) {
                is IRI -> createIRI(subject, db)
                is BlankNode -> createBlankNode(subject, db)
                else -> throw RuntimeException("Unexpected subject type $subject")
            }
            db.commit()
        } catch (ex: Exception) {
            db.rollback()
        }
        db.close()
    }

    private fun createIRI(iri: IRI, db: ODatabaseDocument): OVertex {
        val vertex = db.newVertex(iriClass)
        vertex.setProperty("value", iri.value)
        vertex.save<OVertex>()
        return vertex
    }

    private fun createBlankNode(blankNode: BlankNode, db: ODatabaseDocument): OVertex {
        val vertex = db.newVertex(blankNodeClass)
        vertex.setProperty("label", blankNode.label)
        vertex.save<OVertex>()
        return vertex
    }

    private fun createLangLiteral(langLiteral: LangLiteral, db: ODatabaseDocument): OVertex {
        val vertex = db.newVertex(langLiteralClass)
        vertex.setProperty("value", langLiteral.value)
        vertex.setProperty("langTag", langLiteral.langTag)
        vertex.save<OVertex>()
        return vertex
    }

    private fun createTypedLiteral(typedLiteral: TypedLiteral, db: ODatabaseDocument): OVertex {
        val vertex = db.newVertex(typedLiteralClass)
        vertex.setProperty("value", typedLiteral.value)
        vertex.setProperty("type", typedLiteral.datatypeIRI.value)
        vertex.save<OVertex>()
        return vertex
    }

    fun getIRIs(): Set<IRI> {
        val db = databasePool.acquire()
        val iris = HashSet<IRI>()
        var resultSet = db.browseClass(iriClass)
        while (resultSet.hasNext()) {
            val result = resultSet.next()
            val value = result.getProperty<String>("value")
            iris.add(IRI(value))
        }
        resultSet = db.browseClass(iriEdgeClass)
        while (resultSet.hasNext()) {
            val result = resultSet.next()
            val value = result.getProperty<String>("value")
            iris.add(IRI(value))
        }
        resultSet = db.browseClass(typedLiteralClass)
        while (resultSet.hasNext()) {
            val result = resultSet.next()
            val value = result.getProperty<String>("type")
            iris.add(IRI(value))
        }
        db.close()
        return iris
    }

    override fun getSubjects(): Set<Subject> {
        val db = databasePool.acquire()
        val vertexStream = db.query("SELECT FROM IRI").vertexStream()
        val result = vertexStream.map { vertex ->
            IRI(vertex.getProperty("value"))
        }.collect(Collectors.toSet())
        db.close()
        return result
    }

    override fun statementsFor(subject: Subject): Set<Pair<Predicate, Object>> {
        val results = HashSet<Pair<Predicate, Object>>()
        val db = databasePool.acquire()
        val vertexStream = subjectToVertex(subject, db)
        vertexStream.forEach { subjectVertex ->
            subjectVertex.getEdges(ODirection.OUT).forEach { edge ->
                val predicate = IRI(edge.getProperty("value"))
                val `object` = edge.to
                val resultObject = if (`object`.schemaType.get().name == iriClass) {
                    IRI(`object`.getProperty("value"))
                } else if (`object`.schemaType.get().name == typedLiteralClass) {
                    TypedLiteral(`object`.getProperty("value"), IRI(`object`.getProperty("type")))
                } else if (`object`.schemaType.get().name == langLiteralClass) {
                    LangLiteral(`object`.getProperty("value"), `object`.getProperty("langTag"))
                } else if (`object`.schemaType.get().name == blankNodeClass) {
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
            removeStatement(subject, predicate, `object`, db)
        }
    }

    override fun removeSubject(subject: Subject) {
        val statements = statementsFor(subject)
        val db = databasePool.acquire()
        db.use {
            statements.forEach { statement: Pair<Predicate, Object> ->
                removeStatement(subject, statement.first, statement.second, db)
            }
        }
    }

    private fun removeStatement(subject: Subject, predicate: Predicate, `object`: Object, db: ODatabaseSession) {
        val subjectVertex = subjectToVertex(subject, db).findFirst().get()
        val predicateEdge = subjectVertex.getEdges(ODirection.OUT).first { edge ->
            predicate is IRI && predicate.value == edge.getProperty<String>("value") && vertexAndObjectEqual(edge.to, `object`)
        }
        val objectVertex = predicateEdge.to
        predicateEdge.delete<ORecord>()
        val objectType = objectVertex.getProperty<String>("type")
        if (objectType == langLiteralClass || objectType == typedLiteralClass) {
            objectVertex.delete<ORecord>()
        }
        db.commit()
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
        val vertexType = vertex.schemaType.get().toString()
        return when (`object`) {
            is IRI -> vertexType == iriClass && `object`.value == vertex.getProperty<String>("value")
            is BlankNode -> vertexType == blankNodeClass && `object`.label == vertex.getProperty<String>("label")
            is LangLiteral -> vertexType == langLiteralClass && `object`.langTag == vertex.getProperty<String>("langTag")
                && `object`.value == vertex.getProperty<String>("value")
            is TypedLiteral -> vertexType == typedLiteralClass && `object`.datatypeIRI.value == vertex.getProperty<String>("type")
                && `object`.value == vertex.getProperty<String>("value")
            else -> throw RuntimeException("Unexpected object type $`object`")
        }
    }
}
