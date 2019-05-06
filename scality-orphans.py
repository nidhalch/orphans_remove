#!/usr/bin/env python
import os
import shutil
import time
import sys
import subprocess
import argparse
import datetime
import requests
import calendar
from tqdm import tqdm
import math
import glob
import colorama
from colorama import Fore, Style
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
requests.packages.urllib3.disable_warnings()

class dates():
    def date_set(self):
        self.cal = calendar.Calendar()
        self.today = datetime.date.today()
        self.actual_day = self.today.day
        self.actual_month = self.today.month
        self.actual_year = self.today.year
        self.yesterday = self.today-datetime.timedelta(1)

class Scality_orphans():
    '''
    Get orphans hash list over Https from scatolog-01
    filer-01, filer-02
    geofiler-01-nyc, geofiler-01-sg1, geofiler-01-sv6
    +++ Remove orphans
    '''
    def __init__(self, args):
    #Default Vars
        self.empty_log_file = True #check if log file empty
        self.only_day = False #catch wildcard in date : 2019-04-*
        self.only_mnths = False #catch montj iin date : 2019-*-*
        self.executed = False #exec function once
        self.excluded = False #exclude path if fet error when download
        self.func = False

    #Set date vars
        self.dates = dates()
        self.dates.date_set()
        self.cal = self.dates.cal
        self.today =  self.dates.today
        self.actual_day = self.dates.actual_day
        self.actual_month = self.dates.actual_month
        self.actual_year = self.dates.actual_year
        self.yesterday = self.dates.yesterday

    #Set global vars
        self.filers = ["filer-01", "filer-02"] #list of scality filers
        self.geofilers = ["geofiler-01-sg1", "geofiler-01-nyc", "geofiler-01-sv6"] #list of geofilers
        self.url = "https://scatolog.adm.dc3.dailymotion.com"
        self.log_file_err = ''
        self.username = 'admin' #user to connect on scatolog-01 over https
        self.password = '<ojtazi#jUv8!qua'#password to connect on scatologe over hhtps

    #Call functions
        self.remove = args.remove
        self.date = args.date
        self.remove_orphans=self.remove_orphans()#call remove function
        self.remove_orphans

     #Error funcion :exit function
    def error_exit(self,message):
        print(message)
        sys.exit(3)
     
     #Write message into file
    def write_file(self, file_, message):
         with open(file_, 'w') as in_file:
             in_file.write(message)

     #Excute commande : subprocess
    def cmd_execute(self, command):
        try:
            cmd = subprocess.Popen(command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
            result = cmd.stdout.readlines()
            print(Fore.GREEN + "[Success]") 
        except subprocess.CalledProcessError as exc:
            print("Status : FAIL", exc.returncode, exc.cmd)

    #Count lines on file : used to count orphans on a filer based on generated list
    def count_orphans(self, file_list):
        count_orphans = 0
        with open(file_list) as infp:
            for line in infp:
                count_orphans += 1
        return(count_orphans)

    def start_from_buttom(self, imput_file, output_file, log_file): #imput ==> input
        file_log = open(log_file ,mode='r')
        all_of_it = file_log.read()
        with open(imput_file) as in_file:
            with open(output_file, 'w') as out_file:
                for line in in_file:
                    for line in in_file:
                         if line in all_of_it :
                             tag_found = True
                         else:
                             out_file.write(line)

    #Function to wget file_list of orphans from scatolog-01 over https
    def wget_file(self, URL, filename, username, password, log_file_err,):
        response = requests.get(URL, timeout=120, verify=False, auth=(username, password))
        msg_log = ''
        try:
            response.raise_for_status()
            self.excluded = False
        except requests.exceptions.HTTPError as httperr:
                self.excluded = True
                self.write_file(log_file_err, "Http Error: 404 Client Error: Not Found for url: "+ URL + '\n')
        except requests.exceptions.ConnectionError as err_connect:
                self.excluded = True
                self.write_file(log_file_err, "Error Connecting:",err_connect + '\n')
        except requests.exceptions.Timeout as err_timeout:
                self.excluded = True
                self.write_file(log_file_err, "Timeout Error:",err_timeout + '\n')
        except requests.exceptions.RequestException as err:
                self.excluded = True
                self.write_file(log_file_err, "OOps: Something Else",err + '\n')
        else:
            result = response.text
            total_size = int(response.headers.get('content-length', 0));
            block_size = 1024
            wrote = 0
            with open(filename, 'w+') as file_:
                for data in tqdm(response.iter_content(block_size), total=math.ceil(total_size//block_size) , unit='KB', unit_scale=True):
                    wrote = wrote  + len(data)
                    file_.write(data)
                    #file_.wr/data/orphans_remove/filer-01/2019/04/30/remove_orphans.logite(str(result))
            if total_size != 0 and wrote != total_size:
                print("ERROR, something went wrong")
            print(Fore.GREEN + "[Success]")  

    #This function get all dates since year-01-01 when we using wildcard
    #We iterate over this list to remove orphans of all dates
    def get_url_list(self, actual_year, last_month, actual_month , actual_day, cal):
        display = []
        stop_date = False
        if self.only_day:
            list_month = str(last_month)
            stop_date = True
        if self.only_mnths:
            list_month = range(1, actual_month)
        for mnth in list_month:
            for date in cal.itermonthdates (int(actual_year), int(mnth)):
                display.append(str(date))
                if stop_date:
                    date_stop = str(date)
                    if int(last_month) >= 10:
                        if date_stop.split('-')[1] != last_month:
                            display.remove(str(date))
                    else:
                         if date_stop[6] != last_month:
                             display.remove(str(date))
                if date == self.yesterday:
                    break
        list_url = display
        return(list_url)
 
    #Iterate over dates_list
    def gen_lazy_list_orphans(self, url_path):
        for url in url_path:
            yield url

    def parse_date(self, date):
        url_path = []
        month = date.split('-')[1] #Get the month from date
        day = date.split('-')[2] #Get the day from dare
        if '*' in month: #Check that there is a wildcard "*" in setted date : 2019-*-15
            self.only_mnths = True
            list_url =  self.get_url_list(self.actual_year, month, self.actual_month, self.actual_day, self.cal)
            for url in list_url:
                url_path.append(url)
        elif '*' in day:
            if month == self.actual_month:
                self.actual_month = self.actual_month
            else:
                self.only_day = True
                if int(month) < 10:
                    month = month[1]
                list_url =  self.get_url_list(self.actual_year, month, self.actual_month, self.actual_day, self.cal)
                for url in list_url:
                    url_path.append(url)
        else:
            url_path.append(self.date)
        return url_path

     #Collect orphans function
    def remove_orphans(self):
        self.number = 1
        self.url_path= []
        if self.date:
            self.url_path = self.parse_date(self.date)
        else:
            self.url_path.append(self.yesterday)
        self.url_path = self.gen_lazy_list_orphans(self.url_path)
        for path in self.url_path:
            self.list_exist = False
            self.log_exist = False
            self.excluded = False
            #Get Year,month and day from date
            if self.remove in self.filers or self.remove in self.geofilers:
                self.year = str(path).split('-')[0] #Get year
                self.month = str(path).split('-')[1] #Get month
                self.day = str(path).split('-')[2] #Get day
                self.list_file = 'orphans_' + str(path) + '.list' #Set orphans list file
                dir_dest = '/{}/{}/{}'.format(self.year, self.month, self.day)
                #If filer in  ["filer-01", "filer-02"]
                if self.remove in self.filers:
                    self.filer = self.remove[0:8]
                    self.local_dir = "/data/orphans_remove/{}{}".format(self.filer, dir_dest)
                    self.url = "https://scatolog-01.adm.dc3.dailymotion.com/{}/{}/{}/{}/{}".format(self.filer, self.year, self.month, self.day, self.list_file)
                    if not os.path.exists(self.local_dir):
                        os.makedirs(self.local_dir)
                    if  os.path.isfile(self.local_dir + '/' + self.list_file):
                        self.list_exist = True
                # If filer in ["geofiler-01-sg1", "geofiler-01-nyc", "geofiler-01-sv6"] 
                else:
                    self.filer = self.remove.split('-')[0]
                    self.filer_ordre = self.remove.split('-')[1]
                    self.region = self.remove.split('-')[2]
                    self.local_dir = "/data/orphans_remove/{}-{}-{}{}".format(self.filer, self.filer_ordre,  self.region, dir_dest)
                    self.url = "https://scatolog-01.adm.dc3.dailymotion.com/{}-{}/{}/{}/{}/{}".format(self.filer, self.region, self.year, self.month, self.day, self.list_file)
                    if not os.path.exists(self.local_dir):
                        os.makedirs(self.local_dir)
                    if  os.path.isfile(self.local_dir + '/' + self.list_file):
                        self.list_exist = True
                #Global configuration 
                #Store err log when failed download
                base_logs_dir = '/data/orphans_remove/logs/{}/{}/{}'.format(self.year, self.month, self.actual_day)
                if not os.path.exists(base_logs_dir) :
                    os.makedirs(base_logs_dir)
                self.log_file_err = base_logs_dir + '/remove_orphans.err'
                if  os.path.isfile(self.log_file_err) and not self.executed:
                    open(self.log_file_err, 'w').close()
                    self.executed = True
                #Removing log : logs generated when launching orphans removing
                self.removing_log = self.local_dir + '/' + "remove_orphans.log"
                if  os.path.isfile(self.removing_log):
                        self.log_exist = True
                if self.log_exist:
                    if  os.path.getsize(self.removing_log) > 0 :
                        self.empty_log_file = False
                if not self.list_exist:
                    print(Fore.WHITE + "[Step1] Start downloading list : " + self.url)
                    self.wget_file(self.url, self.local_dir + '/' + self.list_file, self.username, self.password, self.log_file_err)
                    if self.excluded:
                        print(Fore.RED + "Error : cant download file " + self.url)
                elif self.list_exist and not self.empty_log_file and not self.excluded:
                     print(Fore.GREEN + "[found list on server] :" + self.local_dir + '/' + self.list_file)
                     self.count = self.count_orphans(self.local_dir + '/' + self.list_file)
                     if self.count:
                         self.start_from_buttom(self.local_dir + '/' + self.list_file, self.local_dir + '/' + self.list_file + '.temp', self.removing_log)
                         os.rename(self.local_dir + '/' + self.list_file + '.temp', self.local_dir + '/' + self.list_file)
                elif self.list_exist and not self.excluded :
                    print(Fore.GREEN + "[found list on server] :" + self.local_dir + '/' + self.list_file)
                else:
                    self.error_exit('Uknow error')
                if not self.excluded:
                    self.count = self.count_orphans(self.local_dir + '/' + self.list_file)
                    cmd = "/usr/bin/python /data/orphans_remove/remove_orphans.py -r {} -m 5 -i {} -t20 >> {}".format("FILER-01", self.local_dir + '/' + self.list_file, self.removing_log)
                    print(Fore.WHITE + "[Step2] Start removing orphans [{}] on {} of {}".format(self.count, self.remove, path ))
                    self.cmd_execute(cmd)
            else:
                self.error_exit("UKNOWN SERVER NAME !!! \n" + "FIler enabled : ['filer-01', 'filer-02', 'geofiler-01-sg1', 'geofiler-01-nyc', 'geofiler-01-sv6']\n" + "use --help option")
        if os.path.getsize(self.log_file_err) > 0:
            print(Fore.RED + " Warning : See errors on " + self.log_file_err)
def main():
    parser = argparse.ArgumentParser(
        description='Scality orphans',
        add_help=True
    )
    parser.add_argument('--remove', type=str,
                        help='remove orpahns')
    parser.add_argument('--date', type=str,
                        help='date to remvove : Example : /usr/local/bin/scality-orphans.py --remove filer-02 --date 2019-04-10 Or 2019-04-*')
    args = parser.parse_args()
    scality_orphans = Scality_orphans(args)
if __name__ == '__main__':
    main()

