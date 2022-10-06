#!/usr/bin/env python
from zeep import xsd
import time
import csv
import os
import requests
import backoff
from ultipro.helpers import backoff_hdlr
from zeep import Client as ZeepClient
from zeep import Plugin
from zeep.transports import Transport
from lxml import etree
from ultipro.helpers import backoff_hdlr
import requests
import backoff  # Helps handle intermittent 405 errors from server


@backoff.on_exception(
    backoff.expo,
    requests.exceptions.HTTPError,
    max_tries=8,
    on_backoff=backoff_hdlr,
)
class UltiProClient:
    def __init__(
        self,
        username=os.environ.get("UKG_UserName"),
        password=os.environ.get("UKG_Password"),
        client_access_key=os.environ.get("UKG_ClientAccessKey"),
        user_access_key=os.environ.get("UKG_UserAccessKey"),
        base_url="https://service4.ultipro.com/services/",
    ):
        assert username is not None
        assert password is not None
        assert client_access_key is not None
        assert user_access_key is not None
        assert base_url is not None
        self.username = username
        self.password = password
        self.client_access_key = client_access_key
        self.user_access_key = user_access_key
        self.base_url = base_url
        self.authenticate()

    def authenticate(self):
        login_header = {
            "UserName": self.username,
            "Password": self.password,
            "ClientAccessKey": self.client_access_key,
            "UserAccessKey": self.user_access_key,
        }
        endpoint = "LoginService"
        # Log in and get session token
        zeep_client = ZeepClient(f"{self.base_url}{endpoint}")
        result = zeep_client.service.Authenticate(_soapheaders=login_header)
        self.token = result["Token"]
        # Create xsd ComplexType header - http://docs.python-zeep.org/en/master/headers.html
        header = xsd.ComplexType(
            [
                xsd.Element(
                    "{http://www.ultimatesoftware.com/foundation/authentication/ultiprotoken}UltiProToken",
                    xsd.String(),
                ),
                xsd.Element(
                    "{http://www.ultimatesoftware.com/foundation/authentication/clientaccesskey}ClientAccessKey",
                    xsd.String(),
                ),
            ]
        )
        # Add authenticated header to client object
        self.session_header = header(
            UltiProToken=self.token, ClientAccessKey=self.client_access_key
        )
        return True

    def find_people(self, query):
        zeep_client = ZeepClient(f"{self.base_url}{'/EmployeePerson'}")
        response = zeep_client.service.FindPeople(
            _soapheaders=[self.session_header], query=query
        )
        return response["Results"]

    def get_person_by_employee_number(self, employee_number):
        zeep_client = ZeepClient(f"{self.base_url}{'/EmployeePerson'}")
        element = zeep_client.get_element("ns6:EmployeeNumberIdentifier")
        obj = element(EmployeeNumber=employee_number)
        response = zeep_client.service.GetPersonByEmployeeIdentifier(
            _soapheaders=[self.session_header], employeeIdentifier=obj
        )
        return response["Results"]

    def get_global_person_by_employee_number(self, employee_number):
        zeep_client = ZeepClient(f"{self.base_url}{'/employeeglobalnewhire'}")
        element = zeep_client.get_element("ns6:EmployeeNumberIdentifier")
        obj = element(EmployeeNumber=employee_number)
        response = zeep_client.service.GetGlobalEmployeeByEmployeeIdentifier(
            _soapheaders=[self.session_header], employeeIdentifier=obj
        )
        return response["Results"]

    def get_person_by_email_address(self, email_address):
        zeep_client = ZeepClient(f"{self.base_url}{'/EmployeePerson'}")
        element = zeep_client.get_element("ns6:EmailAddressIdentifier")
        obj = element(EmailAddress=email_address)
        response = zeep_client.service.GetPersonByEmployeeIdentifier(
            _soapheaders=[self.session_header], employeeIdentifier=obj
        )
        return response["Results"]

    def update_person(self, person):
        zeep_client = ZeepClient(f"{self.base_url}{'/EmployeePerson'}")
        response = zeep_client.service.UpdatePerson(
            _soapheaders=[self.session_header], entities=person
        )
        return response["Results"]

    def update_global_person(self, person):
        zeep_client = ZeepClient(f"{self.base_url}{'/employeeglobalnewhire'}")
        response = zeep_client.service.UpdateGlobalEmployee(
            _soapheaders=[self.session_header], entities=person
        )
        return response["Results"]

    def log_on_with_token(self):
        # print(inspect.getmembers(client))
        credentials = {"Token": self.token, "ClientAccessKey": self.client_access_key}
        # Log on to get ns5:DataContext object with auth
        zeep_client = ZeepClient(f"{self.base_url}{'BiDataService'}")
        element = zeep_client.get_element("ns5:LogOnWithTokenRequest")
        obj = element(**credentials)
        # print(inspect.getmembers(obj))
        return zeep_client.service.LogOnWithToken(obj)

    def get_report_list(self):
        zeep_client = ZeepClient(f"{self.base_url}{'BiDataService'}")
        context = self.log_on_with_token()
        return zeep_client.service.GetReportList(context)

    def get_report_path_by_name(self, report_name):
        report_list = self.get_report_list()
        return list(
            filter(lambda x: x["ReportName"] == report_name, report_list.Reports.Report)
        )[0]["ReportPath"]

    def get_report_parameters(self, report_path):
        context = self.log_on_with_token()
        zeep_client = ZeepClient(f"{self.base_url}{'BiDataService'}")
        return zeep_client.service.GetReportParameters(report_path, context)

    def __execute_report(self, report_path, delimiter=","):
        session = requests.Session()
        context = self.log_on_with_token()
        session.headers.update({"US-DELIMITER": delimiter})
        transport = Transport(session=session)
        payload = {"ReportPath": report_path}
        zeep_client = ZeepClient(
            f"{self.base_url}{'BiDataService'}", transport=transport
        )
        element = zeep_client.get_element("ns5:ReportRequest")
        obj = element(**payload)
        r = zeep_client.service.ExecuteReport(request=obj, context=context)
        return r["ReportKey"]

    def execute_report(self, report_name, delimiter=","):
        report_path = self.get_report_path_by_name(report_name)
        session = requests.Session()
        context = self.log_on_with_token()
        session.headers.update({"US-DELIMITER": delimiter})
        transport = Transport(session=session)
        payload = {"ReportPath": report_path}
        zeep_client = ZeepClient(
            f"{self.base_url}{'BiDataService'}", transport=transport
        )
        element = zeep_client.get_element("ns5:ReportRequest")
        obj = element(**payload)
        r = zeep_client.service.ExecuteReport(request=obj, context=context)
        return r["ReportKey"]

    def execute_and_retrieve_report(self, report_name, delimiter=","):
        report_path = self.get_report_path_by_name(report_name)
        k = self.__execute_report(report_path, delimiter=delimiter)
        r = self.retrieve_report(k)
        report = r["body"].ReportStream.decode("unicode-escape").split("\r\n")
        csvreader = csv.reader(report)
        headers = next(csvreader)
        output = []
        for row in csvreader:
            if len(row) > 0:
                output.append(
                    dict(
                        map(
                            lambda rowitem: (headers[rowitem], row[rowitem]),
                            range(len(row)),
                        )
                    )
                )
        return output

    def retrieve_report(self, report_key):
        zeep_client = ZeepClient(f"{self.base_url}{'BiStreamingService'}")
        r = zeep_client.service.RetrieveReport(
            _soapheaders={"ReportKey": report_key}
        )
        status = r['header']['Status']
        if status == "Working":
            i = 0
            while ((status != 'Completed') and (i < 30)) : #i<30 is equivalent to 15 minutes worth of sleep. doesn't include time for RetrieveReport to run.
                print('Waiting on report to finish generating...')
                print('Sleeping for 30 seconds')
                time.sleep(30)
                r = zeep_client.service.RetrieveReport(
                    _soapheaders={"ReportKey": report_key}
                )
                status = r['header']['Status']
                i = i+1
        if status == "Completed":
            return r
        if (status != "Completed") or (i > 50):
            raise Exception("Failed to generate report.\nReport Key: {}".format(report_key))
