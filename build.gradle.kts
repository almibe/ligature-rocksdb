plugins {
    id("org.jetbrains.kotlin.jvm").version("1.3.70")
    maven
}

project.group = "org.libraryweasel.ligature"
project.version = "0.1.0-SNAPSHOT"

repositories {
    mavenLocal()
    jcenter()
}

dependencies {
    implementation("org.jetbrains.xodus:xodus-environment:1.3.91")
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.3.4")
    implementation("io.vavr:vavr:1.0.0-alpha-3")
    implementation("org.libraryweasel.ligature:ligature:0.1.0-SNAPSHOT")
    testImplementation("org.libraryweasel.ligature:ligature-test-suite:0.1.0-SNAPSHOT")
    testImplementation("io.kotlintest:kotlintest-runner-junit5:3.4.2")
}

tasks.withType<Test> {
    useJUnitPlatform()
}
