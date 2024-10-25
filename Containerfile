FROM python:3.9.6

RUN apt-get update
RUN apt-get install -y default-jre-headless

RUN pip install pipenv

COPY . DataEngineering
RUN cd DataEngineering
RUN cd DataEngineering; ls -lha
RUN cd DataEngineering; pipenv install --dev

