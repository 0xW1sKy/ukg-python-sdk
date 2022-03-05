#!/usr/bin/env python
from zeep import xsd
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

    def log_on_with_token(self):
        # print(inspect.getmembers(client))
        credentials = {"Token": self.token, "ClientAccessKey": self.client_access_key}
        # Log on to get ns5:DataContext object with auth
        zeep_client = ZeepClient(f"{self.base_url}{'BiDataService'}")
        element = zeep_client.get_element("ns5:LogOnWithTokenRequest")
        obj = element(**credentials)
        # print(inspect.getmembers(obj))
        return zeep_client.service.LogOnWithToken(obj)

    def get_report_list(self, context):
        zeep_client = ZeepClient(f"{self.base_url}{'BiDataService'}")
        return zeep_client.service.GetReportList(context)

    def get_report_path_by_name(self, context, report_name):
        report_list = self.get_report_list(context)
        return list(
            filter(lambda x: x["ReportName"] == report_name, report_list.Reports.Report)
        )[0]["ReportPath"]

    def get_report_parameters(self, report_path, context):
        zeep_client = ZeepClient(f"{self.base_url}{'BiDataService'}")
        return zeep_client.service.GetReportParameters(report_path, context)

    def execute_report(self, context, report_path, delimiter=","):
        session = requests.Session()
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
        context = self.log_on_with_token()
        report_path = self.get_report_path_by_name(context, report_name)
        k = self.execute_report(context, report_path, delimiter=delimiter)
        r = self.retrieve_report(k)
        return r["body"]["ReportStream"].decode("unicode-escape")

    def retrieve_report(self, report_key):
        zeep_client = ZeepClient(f"{self.base_url}{'BiStreamingService'}")
        return zeep_client.service.RetrieveReport(
            _soapheaders={"ReportKey": report_key}
        )
