import sys
import argparse
from exabeamAPI import ExabeamAPI

def parseArguments():
    desc = 'Exabeam Context Table management tool.'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('-v', '--version', help='show program version', action="store_true")
    parser.add_argument('-s', '--server', help='Data Lake server (name or IP)', required=True)
    parser.add_argument('-P', '--port', default=8484, help='Management port (default 8484)')
    #parser.add_argument('-c', '--config', default='~/.exabeam/credentials.conf', help='Configuration file')
    parser.add_argument('-u', '--user', help='API Username')
    parser.add_argument('-p', '--password', help='API Password')
    parser.add_argument('action', help='Management operation to perform (append|create|delete|get|relpace).')
    parser.add_argument('actionArgs', help='append <csv filename>|create <table name>|delete <table name>|get <table name>|replace <csv filename>', nargs='+')
    args = parser.parse_args()

    if args.version:
        print("manageContextTables.py: version 0.1")
        sys.exit(1)

    return args

def main():
    args = parseArguments()
    myAPI = ExabeamAPI(args.server,args.port,args.user,args.password)
    if args.action == 'append':
        print(myAPI.addRecord(args.actionArgs[0],args.actionArgs[1:],False))
    elif args.action == 'create':
        #print(myAPI.createTable(args.actionArgs[0]))
        print('coming soon...')
    elif args.action == 'delete':
        print('coming soon...')
    elif args.action == 'get':
        print(myAPI.getTable(args.actionArgs[0]))
    elif args.action == 'replace':
        print(myAPI.addRecord(args.actionArgs[0],args.actionArgs[1:],True))
    else:
        print('not ready yet')

if __name__ == "__main__":
    main()