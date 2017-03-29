#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri Dec  2 16:33:30 2016

@author: robinlietar
"""

from twisted.internet.protocol import ClientFactory
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor

class TTTClientProtocol(LineReceiver):
    def lineReceived(self, line):
        line = line.strip()
        if line == 'You are connected':
            self.sendLine(self.factory.username)
        else:
            print 'SERVER SAYS:', line

class TTTClientFactory(ClientFactory):
    protocol = TTTClientProtocol 

    def __init__(self, name):
        self.username = name


name = raw_input('Please enter your name: ')
print 'Connecting...'

reactor.connectTCP('35.165.30.118', 8001, TTTClientFactory(name))

reactor.run()