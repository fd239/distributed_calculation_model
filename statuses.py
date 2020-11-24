# coding: utf8

class ClientTaskStatus(object):
    new = 0
    done = 1


class DispatcherTaskStatus(object):
    new = 0
    sent_to_calculator = 1
    calculated = 2
    sent_to_client = 3
    calculation_timeout = 4


class CalculatorStatus(object):
    ready = 0
    busy = 1
