import json 
import random

number_of_logs = 10

IP_addresses = ['192.168.1.1','192.168.1.2','192.168.1.3','192.168.1.4','192.168.1.5']
URL_list = ['GET','POST','DELETE','PUT']
protocol = ['index.html','submit-form','about.html','contact.html','home.html','services.html']
status = ['200','404','302','500','201','204']


def calc_size():
    return 2**random.randint(0,10)

def generate_log(current_date):
    IP = random.choice(IP_addresses)
    User_identifier = '-'
    User_auth = '-'
    Date = current_date.strftime("%Y-%m-%d")
    Method = '+0000'
    URL = random.choice(URL_list)
    Protocol = random.choice(protocol)
    Status = random.choice(status)
    Size = calc_size()

    data = {
        'IP':IP,
        'user_identifier':User_identifier,
        'user_auth':User_auth,
        'date':Date,
        'method':Method,
        'url':URL,
        'protocol':Protocol,
        'status':Status,
        'size':Size
    }

    return data



def generate_web_logs(current_date):
    new_data = []

    for i in range(number_of_logs):
        dummy = generate_log(current_date)
        new_data.append(dummy)


    return new_data


