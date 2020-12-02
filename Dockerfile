FROM python:3.6-slim-stretch

# build-essential is needed to build some libraries (mainly uwsgi and the various database support ones)
# libmariadbclient-dev is needed to build mysqlclient for mysql/mariadb support
RUN apt update && apt install -y build-essential libmariadbclient-dev libpq-dev git

# create uwsgi user for uWSGI to run as (running as root is a Bad Idea, generally)
RUN useradd -ms /bin/bash uwsgi
RUN mkdir /app
RUN chown -R uwsgi /app

# Add system-wide gitconfig file
ADD git_config /etc/gitconfig

# drop into uwsgi user to copy over API files, should ensure proper permissions for these files
USER uwsgi
WORKDIR /app
COPY . /app/

# drop back into root in order to install API and required libraries
USER root
RUN pip install uwsgi
RUN pip install -e .

# finally drop back into uwsgi user to copy final files and run API
USER uwsgi
# Ensure .ssh folder exists, for SSH keys/configuration to be mounted
RUN mkdir ~/.ssh

# Set SSH file permissions and then start API using uwsgi.ini configuration file
CMD ["/bin/bash", "-c", "chmod -R 600 ~/.ssh && uwsgi --ini /app/uwsgi.ini"]
