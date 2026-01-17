FROM python:3.13-slim-bookworm

# ensure we set UTF-8 as the "systemwide" encoding
# this prevents python from mangling non-ASCII paths with surrogate-escape \udcc3 bytes
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8

# build-essential is needed to build psycopg and uwsgi
# git is needed to pull/push file changes
# imagemagick is needed for conversions as part of facsimile upload
# libpq-dev is needed to build psycopg from source
RUN apt update && apt install -y \
    build-essential \
    git \
    imagemagick \
    libpq-dev

# remove default imagemagick policy file, as it's horribly restrictive for our use-case
RUN rm /etc/ImageMagick-6/policy.xml

# set up git user
RUN git config --global user.email is@sls.fi
RUN git config --global user.name sls-deployment

# add SSH configuration file
ADD ssh_config.conf /etc/ssh/ssh_config.d/

# copy over API files
RUN mkdir /app
WORKDIR /app
COPY . /app/

# install API and dependencies
RUN pip install -e .

# start API
CMD ["uwsgi", "--ini", "/app/uwsgi.ini"]
