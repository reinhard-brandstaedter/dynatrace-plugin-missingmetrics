'''
    Dynatrace Missing Metrics Plugin

    This plugin enhances Dynatrace by adding useful metrics about your monitored environment that are not available in other means out of the box.
    E.g.:
    - information on license consumption (Host Units) for agents installed on hosts
'''
from ruxit.api.base_plugin import RemoteBasePlugin
from ruxit.api.exceptions import ConfigException
from ruxit.select_plugins import BaseActivationContext, selectors
import requests, urllib3, json, time, datetime
import logging

logger = logging.getLogger(__name__)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class MissingMetricsPlugin(RemoteBasePlugin):

    def initialize(self, **kwargs):
        logger.info("Config: %s", self.config)

        self.tenant = self.config["tenantUUID"]
        self.apitoken = self.config["apitoken"]
        self.server = "https://localhost:9999/e/"+self.tenant

        self.calc_hostunits = self.config["calc_hostunits"]
        self.calc_hostunits_tag = self.config["calc_hostunits_tag"]

    def query(self, **kwargs):
        if self.calc_hostunits:
            data = self.getHostUnits()
            self.ingestMetrics(data)

    # ingest custom metrics to Dynatrace (using themetrics API as it provides more flexibility)
    def ingestMetrics(self, data):
        apiurl = "/api/v2/metrics/ingest"
        headers = {"Authorization": "Api-Token {}".format(self.apitoken), "Content-Type": "text/plain"}
        url = self.server + apiurl
        for line in data:
            try:
                response = requests.post(url, headers=headers, verify=False, data=line)
                if response.status_code != 202:
                    logger.info("Ingesting metrics failed: {} {}".format("\n".join(response, line)))
            except:
                logger.error("Metric ingestion failed: {}".format(response, line))

        
    def getHostUnits(self):
        # using a slightly shifted time ensures newly added hosts are tagged
        nowutc = datetime.datetime.now(datetime.timezone.utc)
        nowminus10m = nowutc - datetime.timedelta(minutes=10)
        nowminus5m = nowutc - datetime.timedelta(minutes=5)
        ts10 = int(nowminus10m.timestamp()*1000)
        ts5 = int(nowminus5m.timestamp()*1000)

        apiurl = "/api/v1/entity/infrastructure/hosts"
        parameters = {"includeDetails": "false", "startTimestamp": ts10, "endTimestamp": ts5}
        headers = {"Authorization": "Api-Token {}".format(self.apitoken)}
        url = self.server + apiurl

        unitsbytag = {}
        hostsbytag = {}
        try:
            response = requests.get(url, params=parameters, headers=headers, verify=False)
            result = response.json()
            if response.status_code == requests.codes.ok:
                for host in result:
                    hu = host["consumedHostUnits"]
                    hosts = 1

                    split = "other"
                    if "tags" in host:
                        for tag in host["tags"]:
                            if  self.calc_hostunits_tag  in tag["key"]:
                                split = tag["value"]
                    if split in unitsbytag:
                        hu += unitsbytag[split]
                    if split in hostsbytag:
                        hosts += hostsbytag[split]

                    unitsbytag.update({split:hu})
                    hostsbytag.update({split:hosts})
        except:
            logger.error("Error fetching hostunits")
        
        datalines = []
        total = 0
        for split,value in unitsbytag.items():
            datalines.append("threesixty-perf.license.hostunits,{}=\"{}\" {:.2f}".format(self.calc_hostunits_tag,split,value))
            total += value
        datalines.append("threesixty-perf.license.totalhostunits {:.2f}".format(total))

        total = 0
        for split,value in hostsbytag.items():
            datalines.append("threesixty-perf.infra.hosts,{}=\"{}\" {:.2f}".format(self.calc_hostunits_tag,split,value))
            total += value
        datalines.append("threesixty-perf.infra.totalhosts {:.2f}".format(total))
        
        return datalines


