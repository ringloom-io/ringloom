import com.vanniktech.maven.publish.JavaLibrary
import com.vanniktech.maven.publish.JavadocJar
import com.vanniktech.maven.publish.SourcesJar
import org.gradle.jvm.tasks.Jar

plugins {
    java
    id("com.vanniktech.maven.publish") version "0.36.0"
}

repositories {
    mavenCentral()
}

val publicationGroup = providers.gradleProperty("ringloom.mavenGroup").orElse("io.ringloom")
val publicationArtifactId = providers.gradleProperty("ringloom.mavenArtifactId").orElse("ringloom-java-bindings")
val publicationVersion = providers.gradleProperty("ringloom.version").orElse("0.0.0-SNAPSHOT")

group = publicationGroup.get()
version = publicationVersion.get()

fun normalizeOsName(osName: String): String = when {
    osName.startsWith("linux", ignoreCase = true) -> "linux"
    osName.startsWith("mac os", ignoreCase = true) || osName.startsWith("darwin", ignoreCase = true) -> "macos"
    else -> throw GradleException("Unsupported operating system for embedded RingLoom native library: $osName")
}

fun normalizeArchName(archName: String): String = when (archName.lowercase()) {
    "x86_64", "amd64" -> "x86_64"
    "aarch64", "arm64" -> "aarch64"
    else -> throw GradleException("Unsupported architecture for embedded RingLoom native library: $archName")
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(25))
    }
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.13.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

val repoRoot = layout.projectDirectory.dir("../..").asFile.canonicalFile
val nativeLibraryFileName = System.mapLibraryName("ringloom_service")
val embeddedPlatform = "${normalizeOsName(System.getProperty("os.name"))}-${normalizeArchName(System.getProperty("os.arch"))}"
val embeddedNativeResourceDir = "io/ringloom/service/native/$embeddedPlatform"
val configuredEmbeddedNativeLibDir = ((findProperty("ringloom.embeddedNativeLibDir") as String?)
    ?: System.getProperty("ringloom.nativeLibDir"))
    ?.takeIf { it.isNotBlank() }
val generatedResourcesDir = layout.buildDirectory.dir("generated/resources/main")
val embeddedNativeOutputDir = layout.buildDirectory.dir("generated/resources/main/$embeddedNativeResourceDir")
val embeddedNativeSourceFile = providers.provider {
    val nativeLibDir = configuredEmbeddedNativeLibDir?.let(::file) ?: repoRoot.resolve("zig-out/lib")
    nativeLibDir.resolve(nativeLibraryFileName)
}

sourceSets {
    main {
        resources.srcDir(generatedResourcesDir)
    }
}

val buildEmbeddedNativeLibrary = tasks.register<Exec>("buildEmbeddedNativeLibrary") {
    onlyIf { configuredEmbeddedNativeLibDir == null }
    workingDir = repoRoot
    commandLine("zig", "build", "service-c", "-Doptimize=ReleaseSmall")
    inputs.files(
        repoRoot.resolve("build.zig"),
        repoRoot.resolve("build.zig.zon"),
        repoRoot.resolve("include/ringloom_service.h"),
        fileTree(repoRoot.resolve("src/common")),
        fileTree(repoRoot.resolve("src/service"))
    )
    outputs.file(repoRoot.resolve("zig-out/lib").resolve(nativeLibraryFileName))
}

val stageEmbeddedNativeLibrary = tasks.register<Copy>("stageEmbeddedNativeLibrary") {
    dependsOn(buildEmbeddedNativeLibrary)
    from(embeddedNativeSourceFile)
    into(embeddedNativeOutputDir)
    doFirst {
        val sourceFile = embeddedNativeSourceFile.get()
        if (!sourceFile.exists()) {
            throw GradleException("Embedded RingLoom native library not found at $sourceFile")
        }
    }
}

tasks.named<ProcessResources>("processResources") {
    dependsOn(stageEmbeddedNativeLibrary)
}

tasks.withType<Jar>().configureEach {
    if (name == "sourcesJar") {
        dependsOn(stageEmbeddedNativeLibrary)
    }
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
    jvmArgs("--enable-native-access=ALL-UNNAMED")
    systemProperty("ringloom.projectRoot", System.getProperty("ringloom.projectRoot", repoRoot.absolutePath))
    systemProperty("ringloom.brokerBin", System.getProperty("ringloom.brokerBin", repoRoot.resolve("zig-out/bin/ringloom-broker").absolutePath))
    System.getProperty("ringloom.nativeLibDir")?.takeIf { it.isNotBlank() }?.let {
        systemProperty("ringloom.nativeLibDir", it)
    }
    System.getProperty("ringloom.nativeLibPath")?.takeIf { it.isNotBlank() }?.let {
        systemProperty("ringloom.nativeLibPath", it)
    }
}

mavenPublishing {
    coordinates(publicationGroup.get(), publicationArtifactId.get(), publicationVersion.get())
    configure(
        JavaLibrary(
            javadocJar = JavadocJar.Empty(),
            sourcesJar = SourcesJar.Sources(),
        )
    )
    publishToMavenCentral(automaticRelease = true)
    signAllPublications()

    pom {
        name.set("RingLoom Java Bindings")
        description.set("Java FFM bindings for RingLoom service IPC with embedded native Linux x86_64 library.")
        inceptionYear.set("2026")
        url.set("https://github.com/ringloom-io/ringloom")
        licenses {
            license {
                name.set("Apache License, Version 2.0")
                url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                distribution.set("repo")
            }
        }
        developers {
            developer {
                id.set("ringloom-io")
                name.set("RingLoom Devs")
                url.set("https://github.com/ringloom-io")
            }
        }
        scm {
            url.set("https://github.com/ringloom-io/ringloom")
            connection.set("scm:git:https://github.com/ringloom-io/ringloom.git")
            developerConnection.set("scm:git:ssh://git@github.com/ringloom-io/ringloom.git")
        }
    }
}
