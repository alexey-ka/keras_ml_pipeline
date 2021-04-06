# Pull base image
FROM python:3.7
# Set environment varibles
WORKDIR ./app
# Install dependencies
RUN ls
COPY . ./app
RUN pip install -r ./app/requirements.txt
CMD ["python", "./app/main.py"]