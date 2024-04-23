FROM andimajore/mamba_mantic:latest
ARG ACCESS_TOKEN
ENV ACCESS_TOKEN=$ACCESS_TOKEN
RUN apt-get update && apt-get upgrade -y

RUN apt-get update \
    && apt-get install -y \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg \
    lsb-release \
    software-properties-common \
    build-essential

RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -

RUN add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
   lunar \
   stable"

RUN apt-get update && apt-get install -y docker-ce docker-ce-cli containerd.io

RUN apt-get update && apt-get install -y unzip openjdk-11-jre-headless
RUN mamba install python=3.10
RUN mamba upgrade pip
RUN mamba install git

WORKDIR /app/nedrexapi
COPY . ./

RUN git clone --recurse-submodules https://AndiMajore:${ACCESS_TOKEN}@github.com/repotrial/nedrexapi_v2d
RUN #git submodule update --init --recursive

RUN ls ./scripts
#WORKDIR ./scripts
#RUN wget https://github.com/repotrial/MultiSteinerBackend/archive/refs/tags/1.0.zip -O MultiSteinerBackend.zip
#RUN unzip MultiSteinerBackend.zip
#RUN mv MultiSteinerBackend-1.0 MultiSteinerBackend
#RUN rm MultiSteinerBackend.zip
#WORKDIR ../

RUN pip install .