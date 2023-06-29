FROM openjdk:11
RUN sbt package
ADD target/scala-2.13/youtube-data-producer_2.13-0.1.0-SNAPSHOT.jar youtube-data-producer

ENTRYPOINT ["sbt", "run", "youtube-data-producer"]