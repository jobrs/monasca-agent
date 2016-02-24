# stdlib
import logging
import socket
import struct
from urlparse import urljoin
import requests

def retrieve_json(url):
    r = requests.get(url)
    r.raise_for_status()
    return r.json()

# project
DEFAULT_METHOD = 'http'
SUBCONTAINER_PATH = '/api/v1.3/subcontainers/'
CONTAINERS_PATH = '/api/v1.3/containers/'
METRICS_PATH = '/metrics'
DEFAULT_CADVISOR_PORT = 4194
DEFAULT_KUBELET_PORT = 10255
DEFAULT_MASTER_PORT = 8080

log = logging.getLogger('collector')
_kube_settings = {}

def get_kube_settings():
    global _kube_settings
    return _kube_settings


def set_kube_settings(instance):
    global _kube_settings

    host = instance.get("host") or _get_default_router()
    cadvisor_port = instance.get('port', DEFAULT_CADVISOR_PORT)
    kubelet_port = instance.get('kubelet_port', DEFAULT_KUBELET_PORT)
    master_port = instance.get('master_port', DEFAULT_MASTER_PORT)
    method = instance.get('method', DEFAULT_METHOD)
    metrics_url = urljoin('%s://%s:%d' % (method, host, kubelet_port), METRICS_PATH)
    subcontainer_url = urljoin('%s://%s:%d' % (method, host, cadvisor_port), SUBCONTAINER_PATH)
    containers_url = urljoin('%s://%s:%d' % (method, host, cadvisor_port), CONTAINERS_PATH)

    _kube_settings = {
        "host": host,
        "method": method,
        "metrics_url": metrics_url,
        "subcontainer_url": subcontainer_url,
        "containers_url": containers_url,
        "cadvisor_port": cadvisor_port,
        "labels_url": '%s://%s:%d/pods' % (method, host, kubelet_port),
        "master_url_nodes": '%s://%s:%d/api/v1/nodes' % (method, host, master_port),
        "kube_health_url": '%s://%s:%d/healthz' % (method, host, kubelet_port),
        "kubelet_url": '%s://%s:%d/%s' % (method, host, master_port, METRICS_PATH)
    }

    return _kube_settings


def get_kube_labels():
    global _kube_settings
    pods = retrieve_json(_kube_settings["labels_url"])
    kube_labels = {}
    for pod in pods["items"]:
        metadata = pod.get("metadata", {})
        name = metadata.get("name")
        namespace = metadata.get("namespace")
        labels = metadata.get("labels")
        if name and labels and namespace:
            key = "%s/%s" % (namespace, name)
            kube_labels[key] = labels

    return kube_labels

def _get_default_router():
    try:
        with open('/proc/net/route') as f:
            for line in f.readlines():
                fields = line.strip().split()
                if fields[1] == '00000000':
                    return socket.inet_ntoa(struct.pack('<L', int(fields[2], 16)))
    except IOError, e:
        log.error('Unable to open /proc/net/route: %s', e)

    return None
