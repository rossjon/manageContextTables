import urllib.request
import urllib.parse
import ssl
import http.cookiejar
import csv
import json


class ExabeamAPI:
    def __init__(self, serverURL, serverPort, username, password):

        self.serverURL = "https://{}:{}".format(serverURL, serverPort)
        self.port = serverPort
        self.username = username
        self.password = password

        authURL = "{}/api/auth/login".format(self.serverURL)
        authHeaders = {'Accept': 'application/json'}
        authDict = {'username': username, 'password': password}
        postData = urllib.parse.urlencode(authDict).encode('utf-8')
        self.mySSLContext = ssl._create_unverified_context()
        self.myCookies = http.cookiejar.CookieJar()

        req = urllib.request.Request(url=authURL, data=postData, headers=authHeaders)
        response = urllib.request.urlopen(req, context=self.mySSLContext)
        self.myCookies.extract_cookies(response, req)

    def buildURL(self, path):
        return "{}{}".format(self.serverURL, path)

    def executeAPICall(self, url, data=None, method=None):
        if data != None:
            # body = urllib.parse.urlencode(data).encode('utf-8')
            # For now go with json interpretation if we're in this block. we might find later that it needs a more robust
            # implementation as more functions are added.
            body = json.dumps(data).encode('utf-8')
            print(body)
            req = urllib.request.Request(url=url, data=body, method=method,
                                        headers={'Content-type': 'application/json'})
        else:
            req = urllib.request.Request(url=url,method=method)
        self.myCookies.add_cookie_header(req)
        try:
            resp = urllib.request.urlopen(req, context=self.mySSLContext)
            return resp.read()
        except urllib.error.HTTPError as err:
            print(url)
            return err

    def getTable(self, tableName):
        url = self.buildURL("/api/setup/contextTables/{}/records".format(tableName))
        return self.executeAPICall(url,method='GET')

    def addRecord(self, tableName, records, replace=False):
        # stage 1 send new records to server
        with open(records[0]) as newrecords:
            reader = csv.reader(newrecords)
            body = {}
            body['records'] = []
            for row in reader:
                if len(row) == 2:
                    body['records'].append({'key': row[0], 'value': [row[1]]})
                elif len(row) == 1:
                    # Key only table
                    body['records'].append({'key': row[0], 'value': []})
                else:
                    print('raise an exception here the csv is messed up')

        url = self.buildURL("/api/setup/contextTables/{}/changes/add".format(tableName))
        stagingResult = json.loads(self.executeAPICall(url, body, 'POST'))
        print(stagingResult)

        # stage 2 commit changes
        # extract session id from previous result
        url = self.buildURL("/api/setup/contextTables/{}/records".format(tableName))
        body = {}
        body['sessionId'] = stagingResult['sessionId']
        body['replace'] = replace
        commitResult = self.executeAPICall(url, body, 'PUT')
        if isinstance(commitResult, urllib.error.HTTPError):
            print(commitResult)
        else:
            print(json.loads(commitResult))

    def createTable(self,tableName,objectType,tableType,label=None):
        validObjectTypes = ('users','assets','misc')
        validTableTypes  = ('KeyOnly','KeyValue')
        url = self.buildURL("/api/setup/contextTables")
        body= {}
        body['contextTableName'] = tableName
        if objectType in validObjectTypes:
            body['objectType'] = objectType
        else:
            #fail thru to miscellaneous now
            body['objectType'] = 'Miscellaneous'

        if tableType in validTableTypes:
            body['tableType'] = tableType
        else:
            #fall thru to keyonly for now
            body['tableType'] = 'KeyOnly'

        if label != None:
            body['labelAssignment'] = 'Label'
            body['label'] = label
        else:
            body['labelAssignment'] = "None"
            body['label'] = 'null'

        body['connection'] = 'null'
        body['operations'] = []
        body['tableSources'] = []
        body['isLegacyTable'] = False
        body['isEntriesViewable'] = 0
        body['isEditable'] = 0
        body['isEntriesEditable'] = 0
        body['isAdFiltersEditable'] = 0
        return self.executeAPICall(url, body, method='POST')

    def deleteTable(self,tableName):
        url = self.buildURL("/api/setup/contextTables/{}".format(tableName))
        return self.executeAPICall(url,method='DELETE')