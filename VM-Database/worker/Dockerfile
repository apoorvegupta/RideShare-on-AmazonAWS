FROM ubuntu:latest

RUN apt update && apt install -y python3 python3-pip

RUN pip3 install flask requests datetime flask_api flask_sqlalchemy sqlalchemy docker pika kazoo subprocess.run

CMD ["python3", "/src/app.py"]