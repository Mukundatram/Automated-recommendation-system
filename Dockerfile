# using python:3.12.10-slim as base image
FROM python:3.12.10-slim

# set working directory
WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# copy requirements file and install dependencies
COPY requirement.txt .
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir -r requirement.txt

# copy the rest of the application code
COPY . .

# expose the port that the app will run on
EXPOSE 8000

# run the app
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
