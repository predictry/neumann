# Ubuntu
FROM ubuntu:14.04

RUN apt-get update
RUN apt-get upgrade -y

RUN apt-get install software-properties-common python-software-properties -y
RUN add-apt-repository ppa:nginx/stable -y
RUN apt-get update
RUN apt-get install build-essential -y
RUN apt-get install nginx wget -y
RUN apt-get install build-essential -y
RUN apt-get install libssl-dev openssl nano -y
RUN apt-get install supervisor -y
RUN apt-get install libsqlite3-dev -y
RUN apt-get install python2.7 python-pip -y
RUN apt-get install python-setuptools -y
RUN apt-get install python-dev -y
RUN sudo pip install virtualenv

# Python 3.4.2
WORKDIR /tmp/
RUN wget http://www.python.org/ftp/python/3.4.2/Python-3.4.2.tar.xz
RUN tar -xvf Python-3.4.2.tar.xz
WORKDIR /tmp/Python-3.4.2/
RUN ls
RUN ./configure --with-ensurepip=install
RUN make -j `nproc`
RUN make install
RUN rm -rf /tmp/Python-3.4.2*

# Add user
RUN adduser --disabled-password --gecos "" dispatch

# Create app DIR
ENV APP app
ENV APPDIR /${APP}
RUN mkdir -p ${APPDIR}
RUN chown -R dispatch:dispatch ${APPDIR}

USER dispatch

# get code from github
ADD README.md requirements.txt ${APPDIR}/
ADD nginx-app.conf supervisor-app.conf uwsgi.ini uwsgi_params ${APPDIR}/
ADD config.json logging.json ${APPDIR}/
ADD scripts ${APPDIR}/scripts
ADD src ${APPDIR}/src
ADD tests ${APPDIR}/tests
ADD boto.cfg /home/dispatch/.boto.cfg

# Build app env
WORKDIR ${APPDIR}
RUN bash scripts/build-env.sh

USER root

#supervisor
RUN service supervisor restart
RUN echo "daemon off;" >> /etc/nginx/nginx.conf
RUN rm /etc/nginx/sites-enabled/default

RUN ln -s /app/nginx-app.conf /etc/nginx/sites-enabled/
RUN ln -s /app/supervisor-app.conf /etc/supervisor/conf.d/

EXPOSE 80

# Run services via supervisor
CMD ["supervisord", "-n"]