FROM ghcr.io/repotrial/nedrexapi_v2d-base:master
RUN apt-get update && apt-get upgrade -y && apt-get autoclean -y && apt-get autoremove -y && apt-get clean -y

RUN mamba install -c conda-forge graph-tool==2.97 poetry

WORKDIR /app/nedrexapi

RUN mamba create -n bicon python=3.8
RUN mamba run -n bicon pip install git+https://github.com/daisybio/BiCoN.git
RUN mamba run -n bicon pip install click
RUN mamba run -n bicon pip install networkx==2.8.8

COPY pyproject.toml poetry.lock ./
RUN poetry config virtualenvs.create false
RUN poetry install --without dev --no-root

COPY . ./