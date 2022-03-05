import os
import zeep
import logging
import io
from lxml import etree
from zeep import Plugin

# Use this plugin as a kwarg for the zeep class to print SOAP messages
class LoggingPlugin(Plugin):
    def ingress(self, envelope, http_headers, operation):
        print(etree.tostring(envelope, pretty_print=True))
        return envelope, http_headers

    def egress(self, envelope, http_headers, operation, binding_options):
        print(etree.tostring(envelope, pretty_print=True))
        return envelope, http_headers


def backoff_hdlr(details):
    """Prints backoff debug messages"""
    print(
        "Backing off {wait:0.1f} seconds after {tries} tries "
        "calling function {target}".format(**details)
    )


def backoff_hdlr_with_args(details):
    """USE FOR DEBUGGING ONLY - Prints out all details about backoff events,
    including ultipro client object with credentials, to STDOUT
    """
    print(
        "Backing off {wait:0.1f} seconds after {tries} tries "
        "calling function {target} with args {args} and kwargs "
        "{kwargs}".format(**details)
    )


def serialize(response):
    return zeep.helpers.serialize_object(response)


def write_file(report_stream, path):
    """Writes a stream to a file"""
    f = open(path, "w")
    f.write(report_stream)
    f.close()
