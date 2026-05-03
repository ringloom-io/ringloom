plugins {
    java
}

repositories {
    mavenCentral()
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

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
    jvmArgs("--enable-native-access=ALL-UNNAMED")
    systemProperty("ringloom.projectRoot", System.getProperty("ringloom.projectRoot", repoRoot.absolutePath))
    systemProperty("ringloom.nativeLibDir", System.getProperty("ringloom.nativeLibDir", repoRoot.resolve("zig-out/lib").absolutePath))
    systemProperty("ringloom.brokerBin", System.getProperty("ringloom.brokerBin", repoRoot.resolve("zig-out/bin/ringloom-broker").absolutePath))
}
