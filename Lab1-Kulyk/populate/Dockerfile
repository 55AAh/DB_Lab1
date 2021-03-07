FROM python:latest
WORKDIR /populate
ADD requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY *.py ./
CMD ["python", "-u", "main.py"]