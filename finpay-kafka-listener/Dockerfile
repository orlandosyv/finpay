FROM eclipse-temurin:25-jdk-noble AS build

WORKDIR /workspace
COPY .mvn/ .mvn/
COPY mvnw ./
COPY finpay-kafka-listener/pom.xml ./pom.xml
RUN chmod +x mvnw && ./mvnw -B -ntp dependency:go-offline

COPY finpay-kafka-listener/src/ src/
RUN ./mvnw -B -ntp package -DskipTests

FROM eclipse-temurin:25-jre-noble

RUN groupadd --system finpay-listener \
    && useradd --system --gid finpay-listener --home-dir /app finpay-listener

WORKDIR /app
COPY --from=build --chown=finpay-listener:finpay-listener /workspace/target/finpay-kafka-listener-*.jar app.jar

USER finpay-listener
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
