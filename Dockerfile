# start by pulling the python image
FROM python:3.10.8-buster
EXPOSE 4000

# copy the requirements file into the image
COPY ./requirements.txt /app/requirements.txt

# switch working directory
WORKDIR /app

RUN pip3 install cython

RUN /usr/local/bin/python -m pip install --upgrade pip
RUN pip3 install --no-cache-dir wheel 
RUN pip3 install --no-cache-dir pipwin

RUN pip3 install mysql-connector-python
# install the dependencies and packages in the requirements file
RUN pip3 install --no-cache-dir -r requirements.txt


# copy every content from the local file to the image
COPY . /app

# configure the container to run in an executed manner
ENTRYPOINT [ "python" ]

CMD ["app.py" ]


