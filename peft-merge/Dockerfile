FROM python:3.9

RUN pip install transformers==4.37.0
RUN pip install torch==1.13.1
RUN pip install determined==0.35.0
RUN pip install peft==0.4.0
RUN pip install huggingface-hub==0.24.0

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY peft_merge.py /app
