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
COPY --from=build /app/target/weather-station-mock-1.0-SNAPSHOT-jar-with-dependencies.jar app.jar

# Define environment variables with defaults
ENV KAFKA_BROKER="localhost:9092"
ENV TOPIC_NAME="weather-stations"
ENV STATION_ID="1"
ENV MODE="random"

# Run the application
CMD ["java", "-jar", "app.jar"]
