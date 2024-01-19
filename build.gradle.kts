import java.util.Calendar.YEAR

plugins {
    id("java")
    id ("com.google.protobuf") version "0.9.2"
    id("com.diffplug.spotless") version "6.17.0"
}

group = "org.epics"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.9.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.9.2")
    implementation("org.apache.parquet:parquet-protobuf:1.13.1")
    implementation("org.apache.parquet:parquet-hadoop:1.12.3")
    implementation("org.apache.hadoop:hadoop-client:3.3.2")
    implementation("org.apache.hadoop:hadoop-common:3.3.2")

}

protobuf {
    // Configure the protoc executable
    protoc {
        // Download from repositories
        artifact = "com.google.protobuf:protoc:3.17.3"// matches the version in parquet-protobuf
    }
}

spotless {
    java {
        // don't need to set target, it is inferred from java
        // apply a specific flavor of google-java-format
        palantirJavaFormat()
        // fix formatting of type annotations
        formatAnnotations()
        removeUnusedImports()
        trimTrailingWhitespace()
        endWithNewline()
        targetExclude(fileTree("$buildDir/generated") { include("**/*.java") })
        // make sure every file has the following copyright header.
        // optionally, Spotless can set copyright years by digging
        // through git history (see "license" section below)
        licenseHeader("/* (C)$YEAR */")
    }
}

tasks.getByName<Test>("test") {
    minHeapSize = "1024m"
    maxHeapSize = "5G"
    useJUnitPlatform()
}