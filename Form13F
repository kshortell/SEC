# Script to import the holdings information from SEC form 13F-HR which is for institutional investment holdings
# ASSUMES TABLES ARE ALREADY CREATED IN MySQL
# Import necessary pacakages, define url creator function

import requests, time, re, pymysql, sys
from io import StringIO
from datetime import datetime
import pandas as pd
pd.options.display.float_format = "{:,.2f}".format
import numpy as np
import sqlalchemy as sql
import urllib.parse
from bs4 import BeautifulSoup
get_ipython().run_line_magic('matplotlib', 'inline')

def make_url(base_url, comp):
    url = base_url
    for r in comp:
        url = '{}/{}'.format(url, r)
    return url

print('Imports Complete')

# Using the daily index files on the SEC website, create a list of daily index file links
# Speed bumps (time.sleep) are built in per the request of the SEC

base_url = r"https://www.sec.gov/Archives/edgar/daily-index"
year = '2020'
year_url = make_url(base_url, [ year , 'index.json'])
print('-'*100)
print('Links are for the year: {}'.format(year))
content = requests.get(year_url).json()
master_idx_list = []
for item in content['directory']['item'][0:4]:
    print('Pulling links for Quarter: {}'.format(item['name']))
    qtr_url = make_url(base_url, [year, item['name'], 'index.json'])
    qtr_content = requests.get(qtr_url).json()
    time.sleep(.1)
    
    # list all master index files (only one with a delimiter).
    for file in qtr_content['directory']['item'][0:]:
        if "master" in file['name']:
            file_url = make_url(base_url, [year, item['name'], file['name']])
            master_idx_list.append(file_url)
            time.sleep(.1)
print('Pulled all YTD links.')

# Pull index file links for dates that have not been processed yet

engine = sql.create_engine('mysql+pymysql://ksho:Magnum10%21@localhost/sec') #refresh if server stopped?
connection = engine.connect()
stmt = 'SELECT * FROM idx_dates'
date_sql = pd.read_sql_table('idx_dates', con = connection, index_col = 'Date')
new_dates = []
for link in master_idx_list:
    if link not in date_sql['Link'].values:
        new_dates.append(link)
if len(new_dates) >= 1:
    print('New dates are:')
    for date in new_dates:
        print(date)
else:
    print('Links are up to date. Exiting program...')
#     sys.exit()

# Parse through index files to extract links to forms filed that day, add link for json, create master form df

idx_dfs = []
for k in new_dates:
    content = requests.get(k).content
    # might be able to combine these into the read_csv?
    start_index = content.find(b'CIK')
    clean_content = content[start_index:].decode('utf-8')
    
    # df = pd.read_csv(StringIO(data[re.search('--*',data).end():]),sep='|') from comment on part 2 of video series
    df = pd.read_csv(StringIO(clean_content), sep = "|", skiprows = [1], parse_dates = ['Date Filed'])
    df['Link'] = "https://www.sec.gov/Archives/" + df['File Name'].str.replace("-", "").str.replace(".txt", "/index.json")
    idx_dfs.append(df)
    print("Processed: " + k)
    time.sleep(.1)

all_forms = pd.concat(idx_dfs, axis = 0, ignore_index = True)
all_forms.tail(5)

# Pull the json for all form 13F-HR and create a dictionary of xml links (2) for each filing
# no_hold list is made up of jsons without a holdings xml file, only the primary_doc.xml is present (do something with it?)
# runtime is ~120 links per minute

start_time = time.time()
json_list = [all_forms['Link'][x] for x in all_forms.index if all_forms['Form Type'][x] == '13F-HR']
link_list = []
no_hold = []
lcounter = 0
for json in json_list:
    decode = requests.get(json).json()
    xml_dict = {}
    xml_count = 0
    for dic in decode['directory']['item'][0:]:
        for v in dic.values(): 
            if ".xml" in v.lower():
                xml_count += 1
    for dic in decode['directory']['item'][0:]:
        if dic['name'] == "primary_doc.xml" and xml_count > 1:
            xml_dict['doc_xml'] = json.replace('index.json', '') + dic['name']
        elif ".xml" in dic['name'].lower() and dic['name'] != "primary_doc.xml":
            xml_dict['hold_xml'] = json.replace('index.json', '') + dic['name']
    if xml_dict:
        link_list.append(xml_dict)
    else:
        no_hold.append(lcounter)
        no_hold.append('https://www.sec.gov' + decode['directory']['name'])
    lcounter += 1
    if lcounter % 100 == 0:
        print("Processed {} 13F-HR forms".format(lcounter))
    
    time.sleep(.1)
    
print("\nFiles extracted, %.2f minutes" % ((time.time() - start_time)/60))
print("_"*100)
hold = sum([1 for dic in link_list if "hold_xml" in dic.keys()])
doc = sum([1 for dic in link_list if "doc_xml" in dic.keys()])
print('Number of json links: ' + format(len(json_list)))
print('Total number of dictionaries: ' + str(len(link_list)))
print("Hold count is " + format(hold) + ", " + format(len(link_list)-hold) + " are missing")
print("Doc count is " + format(doc) + ", " + format(len(link_list)-doc) + " are missing")
print('JSON links without a holdings xml link: ' + format(len(no_hold)/2))
print('_'*100)

# loop through link_list of xml links, pull out holdings and company information
# stitch holdings df and company df together, append that to a master holdings df
# runtime is about 60-65 links per minute

start_time = time.time()
loop_no = 0
hold_list = []
append_list = []
bad_list=[]
for index, dic in enumerate(link_list):
    if 'hold_xml' in dic.keys() and dic['hold_xml'] != np.nan:
        hlink = dic['hold_xml']
        hreq = requests.get(hlink).content
        hsoup = BeautifulSoup(hreq, 'lxml')

        for holding in hsoup.find_all(re.compile('.+nfotable')): 

            hold_dict = {}
            hold_dict['name'] = holding.find(re.compile('.+ameofissuer')).text
            hold_dict['CUSIP'] = holding.find(re.compile('.+usip')).text
            hold_dict['class'] = holding.find(re.compile('.+itleofclass')).text
            hold_dict['mkt_val'] = holding.find(re.compile('.+alue')).text #Can this be pulled in as a number?
            hold_dict['shares'] = holding.find(re.compile('.+shprnamt')).text
            hold_dict['type'] = holding.find(re.compile('.+shprnamttype')).text
            
            hold_list.append(hold_dict)
    else:
        bad_list.append(dic) 
 
    if 'doc_xml' in dic.keys() and dic['doc_xml'] != np.nan:
        dlink = dic['doc_xml']
        dreq = requests.get(dlink).content
        dsoup = BeautifulSoup(dreq, 'lxml')    

        for item in dsoup.find_all('edgarsubmission'):
            
            comp_dict = {}
            comp_dict['company'] = item.find('name').text
            comp_dict['CIK'] = item.find('cik').text
            comp_dict['form'] = item.find('submissiontype').text
            comp_dict['period'] = item.find('periodofreport').text
            comp_dict['sig_date'] = item.find('signaturedate').text
    else:
        bad_list.append(dic)
    
    if loop_no % 500 == 0:
        print("Files extracted, %f minutes" % ((time.time() - start_time)/60))
    
    hold_df = pd.DataFrame(hold_list)
    hold_df[['mkt_val', 'shares']] = hold_df[['mkt_val', 'shares']].astype('float')
    comp_df = pd.DataFrame(comp_dict, index = [0])        
    comp_df[['period', 'sig_date']] = comp_df[['period', 'sig_date']].apply(pd.to_datetime, format = '%m-%d-%Y')
    time.sleep(.1)
    stitch_df = pd.concat([comp_df,hold_df], axis = 1).ffill()
    append_list.append(stitch_df)
    hold_list = []
    loop_no += 1
    
full_df = pd.concat(append_list, axis = 0, ignore_index = True)
print("\n")
print(full_df.info())
display(full_df.sample(10))
if len(bad_list) > 0:
    print("Bad dictionaries:")
    for b in bad_list:
        print(b)

# Create MySQL connection, append recent extracted holdings to SQL table (hold13f)

connection = engine.connect()
sqltypes = {'company':sql.types.Text(), 'CIK':sql.types.Text(), 'form':sql.types.Text(), 'period':sql.types.Date(), 
            'sig_date':sql.types.Date(), 'name':sql.types.Text(), 'CUSIP':sql.types.Text(),'class':sql.types.Text(), 
            'mkt_val':sql.types.Float(), 'shares':sql.types.Float(), 'type':sql.types.Text()}
full_df.to_sql('hold13f', con = connection, if_exists = 'append', index = True, dtype = sqltypes)
print("SQL 13F holdings updated.")

# Append list of all daily submitted forms to SQL table (all_forms)

connection = engine.connect()
all_forms_types = {'CIK':sql.types.Integer(), 'Company Name':sql.types.Text(), 'Form Type':sql.types.Text(), 
                   'Date Filed':sql.types.Date(), 'File Name':sql.types.Text(), 'Link':sql.types.Text()}
all_forms.to_sql('all_forms', con = connection, if_exists = 'append', index = True, dtype = all_forms_types)
stmt = 'SELECT * FROM all_forms LIMIT 10'
results = connection.execute(stmt).fetchall()
print('SQL all_forms table updated.')

# Strip date from each link in the index file list, create a df, append to SQL table (idx_dates)

mdate_list = []
for m in new_dates:
    datetuple = []
    mdate = m.split('/')[-1].split('.')[1]
    mdate_dt = datetime.strptime(mdate, "%Y%m%d")
    datetuple = [m, mdate_dt]
    mdate_list.append(datetuple)
idx_df = pd.DataFrame(mdate_list, columns = ['Link', 'Date']).set_index('Date')
connection = engine.connect()
idx_df.to_sql('idx_dates', con=connection, if_exists = 'append', index=True, dtype={'Link':sql.types.Text()})
print('Added latest dates to SQL processed date list.')
