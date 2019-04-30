#!/usr/bin/env python3
import os
import shlex
import shutil
import time
import sys
import datetime
import subprocess
import argparse
import requests
import calendar
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

class Scality_orphans():                                                        
    '''                                                                         
    Collect orphans hash for each filer :                                       
    filer-01, filer-02                                                          
    geofiler-01-nyc, geofiler-01-sg1, geofiler-01-sv6                           
    '''                                                                         
    def __init__(self, args):                                                   
    #Default Vars                                                               
        self.today =  datetime.datetime.today()         
        self.timestamps = str(self.today.strftime("%M-%s"))
        self.today = str(self.today.strftime("%Y-%m-%d")) #today date            
        self.yesterday = datetime.datetime.now() - datetime.timedelta(days = 1) 
        self.yesterday = str(self.yesterday.strftime("%Y-%m-%d")) #Yesterday date
        self.filers = ["filer-01", "filer-02"] #list of scality filers
        self.geofilers = ["geofiler-01-sg1", "geofiler-01-nyc", "geofiler-01-sv6"] #list of geofilers
        self.url = "https://scatolog.adm.dc3.dailymotion.com"
        self.remove = args.remove
        self.empty_log_file = True
        self.list_exist = False
        self.date = args.date
        self.func = ''
        self.only_day = False
        self.only_mnths = False
        self.log_exist = False
        self.username = 'admin'
        self.password = 'xxxxxxx'
        #self.dryrun = args.dry-run
    #Call functions                                                             
        self.remove_orphans=self.remove_orphans()                             
        self.remove_orphans                                                   
   
     #Error funcion                                                              
    def error_exit(self,message):                                               
        print(message)                                                          
        sys.exit(3)                                                             

    def cmd_execute(self, command):
        try:
            cmd = subprocess.Popen(command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
            result = cmd.stdout.readlines()
        except subprocess.CalledProcessError as exc:
            print("Status : FAIL", exc.returncode, exc.cmd)

    def count_orphans(self, file_list):
        count_orphans = 0
        with open(file_list) as infp:
            for line in infp:
                count_orphans += 1
        return(count_orphans)

    def get_last_line(self, file_log):
        if  os.path.getsize(file_log) > 0 :
            with open(file_log, "r") as log_file:
                last_line = log_file.readlines()[-1]
                fields = last_line.split()
                if len(fields) >= 7:
                    last_line = fields[6]
                    return(last_line)
      
    def start_from_buttom(self, imput_file, output_file, TAG):
        tag_found = False
        with open(imput_file) as in_file:
            with open(output_file, 'w') as out_file:
               for line in in_file:
                   if not tag_found:
                       if line.strip() == TAG:
                           tag_found = True
                       else:
                           out_file.write(line)             

    def wget_file(self, URL, filename, username, password):
        try:
            response = requests.get(URL, timeout=120, verify=False, auth=(username, password))
            #response.raise_for_status()
        except requests.exceptions.HTTPError as httperr:
            print ("Http Error:",httperr)
        except requests.exceptions.ConnectionError as err_connect:
            print ("Error Connecting:",err_connect)
        except requests.exceptions.Timeout as err_timeout:
            print ("Timeout Error:",err_timeout)
        except requests.exceptions.RequestException as err:
            print ("OOps: Something Else",err)
        else:
            result = response.text
            with open(filename) as file_:
                file_.write(str(result))

    def get_url_list(self):
        global list_url
        display = []
        cal = calendar.Calendar()
        dt = datetime.datetime.today()
        actual_month = dt.month
        actual_year = dt.year
        if self.only_day:
            list_month = str(actual_month)
        if self.only_mnths:
            actual_month = int(actual_month) + 1
            list_month = range(1, actual_month )
        for mnth in list_month:
            for date in cal.itermonthdays(actual_year, int(mnth)):
                if date != 0:
                    if int(mnth) >= 10 and date >= 10:
                        display.append('{}-{}-{}'.format(actual_year, mnth, date))
                    elif int(mnth) >= 10 and date < 10:
                        display.append('{}-{}-0{}'.format(actual_year, mnth, date))
                    elif int(mnth) < 10 and date >= 10:
                        display.append('{}-0{}-{}'.format(actual_year, mnth, date))
                    else:
                        display.append('{}-0{}-0{}'.format(actual_year, mnth, date))
                    if int(mnth) == dt.month and date == dt.day:
                        break
        list_url = display

    def gen_lazy_list_orphans(self, url_path):
        for url in url_path:
            yield url

     #Collect orphans function                                                  
    def remove_orphans(self):
        self.number = 1
        self.url_path = []
        if self.date :
            self.month = self.date.split('-')[1]
            self.day = self.date.split('-')[2]
            if '*' in self.month:
                self.only_mnths = True
                self.get_url_list()
                for url in list_url:
                    self.url_path.append(url)
            elif '*' in self.day:
                self.only_day = True
                self.get_url_list()
                for url in list_url:
                    self.url_path.append(url)
            else:
                self.url_path.append(self.date)
        else:
            self.url_path.append(self.yesterday)
        self.url_path = self.gen_lazy_list_orphans(self.url_path) 
        for path in self.url_path:
            if self.remove in self.filers or self.remove in self.geofilers:
                self.year = path.split('-')[0]
                self.month = path.split('-')[1]
                self.day = path.split('-')[2]
                self.list_file = 'orphans_' + path + '.list'
                dir_dest = '/' + self.year + '/' + self.month + '/' + self.day
                if self.remove in self.filers:
                    self.filer = self.remove[0:8]
                    self.local_dir = "/data/orphans_remove/{}{}".format(self.filer, dir_dest)
                    self.removing_log = self.local_dir + '/' + "remove_orphans.log"
                    self.url = "https://scatolog-01.adm.dc3.dailymotion.com/{}/{}/{}/{}/{}".format(self.filer, self.year, self.month, self.day, self.list_file)
                    if not os.path.exists(self.local_dir):
                        os.makedirs(self.local_dir)
                    if  os.path.isfile(self.local_dir + '/' + self.list_file):
                        self.list_exist = True
                else:
                    self.filer = self.remove.split('-')[0]
                    self.filer_ordre = self.remove.split('-')[1]
                    self.region = self.remove.split('-')[2]
                    self.local_dir = "/data/orphans_remove/{}-{}-{}{}".format(self.filer, self.filer_ordre,  self.region, dir_dest)
                    self.removing_log = self.local_dir + '/' + "remove_orphans.log"
                    self.url = "https://scatolog-01.adm.dc3.dailymotion.com/{}-{}/{}/{}/{}/{}".format(self.filer, self.region, self.year, self.month, self.day, self.list_file)
                    if not os.path.exists(self.local_dir):
                        os.makedirs(self.local_dir)
                    if  os.path.isfile(self.local_dir + '/' + self.list_file):
                        self.list_exist = True
                if  os.path.isfile(self.removing_log):
                        self.log_exist = True
                if self.log_exist:
                    if  os.path.getsize(self.removing_log) > 0 :
                        self.empty_log_file = False
                        self.func = self.get_last_line(self.removing_log)
                if self.count_orphans :
                    if not self.list_exist:
                        print('Start downloading_file')
                        self.wget_file(self.url, self.local_dir + '/' + self.list_file, self.username, self.password)
                    elif self.list_exist and not self.empty_log_file:
                        start_from_buttom(self.local_dir + '/' + self.list_file, self.local_dir + '/' + self.list_file + '.temp', self.func)
                        os.rename(self.local_dir + '/' + self.list_file + '.temp', self.local_dir + '/' + self.list_file)
                        os.rename(self.removing_log, self.removing_log + '.' + self.number)
                        self.number += 1     
                    elif self.empty_log_file:
                        print("found file")
                    else:
                        print("error")
                    cmd = "/usr/bin/python remove_orphans.py -r {} -m 5 -i {} -t20 > {}".format("FILER-01", self.local_dir + '/' + self.list_file, self.removing_log)
                    print(cmd)
            else:
                self.error_exit("UKNOWN SERVER NAME !!! \n" + "FIler enabled : ['filer-01', 'filer-02', 'geofiler-01-sg1', 'geofiler-01-nyc', 'geofiler-01-sv6']\n" + "use --help option")
def main():
    parser = argparse.ArgumentParser(
        description='Scality orphans',
        add_help=True
    )
    parser.add_argument('--remove', type=str,
                        help='remove orpahns')
    parser.add_argument('--date', type=str,
                        help='date to remvove : Example : /usr/local/bin/scality-orphans.py --remove filer-02 --date 2019-04-10 Or 2019-04-*')
 #    parser.add_argument('--dry-run', type=store_true,
  #                      help='Simulate supression of orphans')
    args = parser.parse_args()
    scality_orphans = Scality_orphans(args)
if __name__ == '__main__':
    main()

