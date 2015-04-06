FROM qnib/terminal
MAINTAINER "Christian Kniep <christian@qnib.org>"

RUN rm /etc/supervisord.d/sshd.ini && \
    rm /etc/consul.d/check_sshd.json
RUN yum install -y python-zmq python-pip && \
    pip install neo4jrestclient pyyaml
ADD etc/supervisord.d/inventory.ini /etc/supervisord.d/
ADD opt/qnib/inventory/bin/inventory.py /opt/qnib/inventory/bin/
ADD etc/consul.d/check_inv.json /etc/consul.d/
