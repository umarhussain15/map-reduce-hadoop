FROM  ubuntu:18.04

RUN apt update && apt install -y python3 ant openssh-client git openjdk-8-jdk openjdk-8-jre python3-requests
RUN echo 2 | update-alternatives --config java
RUN update-alternatives --display java
ENV PATH $PATH:/usr/lib/jvm/java-8-openjdk-amd64/bin
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
COPY src /a2/src
COPY build.xml /a2/build.xml
COPY ivy.xml /a2/ivy.xml
RUN cd /a2 && ant && ant runAllSolutions
#COPY res/sampleData/Taxi/yellow_tripdata_2016-06.csv /a2/res/sampleData/Taxi/yellow_tripdata_2016-06.csv