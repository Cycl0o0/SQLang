plugins {
    kotlin("jvm") version "1.9.22"
    application
}

group = "com.sqlang"
version = "0.1.0"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))
}

application {
    mainClass.set("com.sqlang.MainKt")
}

sourceSets {
    main {
        kotlin {
            srcDir("src")
        }
    }
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    // jvmToolchain(17)
}

tasks.jar {
    manifest {
        attributes["Main-Class"] = "com.sqlang.MainKt"
    }
    from(configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) })
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}