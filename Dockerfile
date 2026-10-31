# One image for the whole system; the role is chosen at runtime by SPRING_PROFILES_ACTIVE.
#
#   docker build -t nosql-db .
#   docker run -e SPRING_PROFILES_ACTIVE=node        nosql-db
#   docker run -e SPRING_PROFILES_ACTIVE=coordinator nosql-db
#   docker run -e SPRING_PROFILES_ACTIVE=gateway -p 8078:8078 nosql-db
#
# ---- build stage: build the whole reactor, produce the single app jar ----
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
COPY Node Node
COPY BootstrappingNode BootstrappingNode
COPY DBMS DBMS
COPY app app
# Not built into the image (nothing in `app` depends on it, so `-pl app -am` skips it), but the root pom
# declares it as a module and Maven refuses to start if a declared module's directory is missing.
COPY ycsb-binding ycsb-binding
RUN mvn -q -B -DskipTests -pl app -am package

# ---- run stage ----
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /build/app/target/nosql-db.jar app.jar
# Seed the admin registry so a node-role container authenticates on a fresh volume.
COPY Node/data/info.json data/info.json
ENTRYPOINT ["java", "-jar", "app.jar"]
