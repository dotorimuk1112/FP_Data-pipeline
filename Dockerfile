FROM apache/airflow:2.5.1

USER supersetuser

RUN useradd admin
RUN apt-get update && apt-get install -y build-essential
RUN apt-get install -y default-libmysqlclient-dev

# Set the timezone to Asia/Seoul
ENV TZ=Asia/Seoul

# Install the tzdata package and set the timezone
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

CMD ["superset", "run", "-p", "8088", "-h", "0.0.0.0"]
