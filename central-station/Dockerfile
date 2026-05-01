# Stage 1: Build the application
FROM maven:3.9.6-eclipse-temurin-21 AS build
WORKDIR /app
# Copy pom.xml and download dependencies
COPY pom.xml .
RUN mvn dependency:go-offline
# Copy source code and build the application
COPY src ./src
RUN mvn clean package -DskipTests

# Stage 2: Run the application
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
# Copy the built fat JAR from the build stage
COPY --from=build /app/target/central-station-0.0.1-SNAPSHOT.jar app.jar

# Define default environment variables
ENV SPRING_KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
ENV APP_TOPIC_INPUT="weather.telemetry.v1"
ENV APP_TOPIC_ALERTS="weather.alerts.rain.v1"

# Run the application
CMD ["java", "-jar", "app.jar"]
