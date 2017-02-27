# (C) Copyright 2015 Hewlett Packard Enterprise Development Company LP


class AgentException(Exception):
    def __init__(self, metric=None):
        super(AgentException, self).__init__()
        self.metric = metric


class Infinity(AgentException):
    pass


class UnknownValue(AgentException):
    pass


class CheckException(AgentException):
    pass


class NaN(CheckException):
    pass


class PathNotFound(Exception):
    pass
