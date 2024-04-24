FROM andimajore/mamba_mantic:latest
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

RUN apt-get update && apt-get install -y docker-ce docker-ce-cli containerd.io zip unzip openjdk-11-jre-headless

RUN mamba install python=3.10
RUN mamba upgrade pip
RUN mamba install -c conda-forge graph-tool poetry

WORKDIR /app/nedrexapi

RUN mamba create -n bicon python=3.8
RUN mamba run -n bicon pip install git+https://github.com/biomedbigdata/BiCoN.git click networkx==2.8.8

COPY pyproject.toml poetry.lock ./
RUN poetry config virtualenvs.create false
RUN poetry install --no-dev

COPY . ./