/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.almibe.ligature.store

import com.orientechnologies.orient.core.db.ODatabasePool
import com.orientechnologies.orient.core.db.document.ODatabaseDocument
import com.orientechnologies.orient.core.record.OEdge
import com.orientechnologies.orient.core.record.OVertex
import org.almibe.ligature.*

class OrientDBLigatureStore(val databasePool: ODatabasePool): Model {
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
                val edge = subjectVertex.addEdge(objectVertex, "IRI")
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

    override fun getIRIs(): Set<IRI> {
        val db = databasePool.acquire()
        val iris = HashSet<IRI>()
        val resultSet = db.browseClass("IRI")//db.query("SELECT FROM IRI")
        while (resultSet.hasNext()) {
            val result = resultSet.next()
            val value = result.getProperty<String>("value")
            iris.add(IRI(value))
        }
        db.close()
        return iris
    }

    override fun getLiterals(): Set<Literal> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getObjects(): Set<Object> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getPredicates(): Set<Predicate> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getSubjects(): Set<Subject> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun statementsFor(subject: Subject): Set<Pair<Predicate, Object>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}
