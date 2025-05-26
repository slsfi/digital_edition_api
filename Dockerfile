FROM python:3.13-slim

# ensure we set UTF-8 as the "systemwide" encoding
# this prevents python from mangling non-ASCII paths with surrogate-escape \udcc3 bytes
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8

# build-essential is needed to build some libraries (mainly uwsgi and the various database support ones)
# git is needed to pull/push file changes
# imagemagick is needed for conversions as part of facsimile upload
# libmariadb-dev is needed to build mysqlclient for mysql/mariadb support
# libpq-dev is needed for proper postgresql support
# pkg-config is required to build mysqlclient
RUN apt update && apt install -y \
    build-essential \
    git \
    imagemagick \
    libmariadb-dev \
    libpq-dev \
    pkg-config

# create uwsgi user for uWSGI to run as (running as root is a Bad Idea, generally)
RUN useradd -ms /bin/bash uwsgi
RUN mkdir /app
RUN chown -R uwsgi /app

# remove default imagemagick policy file, as it's horribly restrictive for our use-case
RUN rm /etc/ImageMagick-6/policy.xml

# drop into uwsgi user to copy over API files, should ensure proper permissions for these files
USER uwsgi
WORKDIR /app
COPY . /app/

# drop back into root in order to install API and required libraries
USER root
RUN pip install -e .
RUN chown -R uwsgi /app


# relocate SSH key and fix permissions
RUN mkdir -p /home/uwsgi/.ssh
RUN mv /app/ssh_key /home/uwsgi/.ssh/id_rsa
RUN chown -R uwsgi:uwsgi /home/uwsgi/.ssh
RUN chmod 600 /home/uwsgi/.ssh/id_rsa

# finally drop back into uwsgi user to copy final files and run API
USER uwsgi

# scan SSH host keys for github.com
RUN ssh-keyscan github.com >> ~/.ssh/known_hosts

# set up git user
RUN git config --global user.email is@sls.fi
RUN git config --global user.name sls-deployment

CMD ["uwsgi", "--ini", "/app/uwsgi.ini"]
