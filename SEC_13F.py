#!/usr/bin/env python
# coding: utf-8

# Script to import the holdings information from SEC form 13F-HR

import requests, time, re, pymysql, datetime, yaml, json
from io import StringIO
import pandas as pd
pd.options.display.float_format = "{:,.2f}".format
import sqlalchemy as sql
from sqlalchemy import Table, Column
from sqlalchemy.types import DateTime, Date,Float, Integer, Text, VARCHAR, TypeDecorator
import urllib.parse
from bs4 import BeautifulSoup
print('Imports Complete')

def make_url(base_url, comp):
    url = base_url
    for r in comp:
        url = '{}/{}'.format(url, r)
    return url

def speed_bump(delay=.1):
    """Time delay requested by the SEC for accessing its website.
    
    https://www.sec.gov/developer (Fair Access)
    
    Args:
        delay (float): time delay. Defaults to 1/10th of a second."""
    time.sleep(delay)

def pull_link_list(year=datetime.date.today().year, prior_years=None):
    """Creates a list of url links to the daily master index files on the SEC website.
    
    The United States Securities and Exchange Commission's database website EDGAR 
    produces five versions of a file summarizing all of the forms submitted
    to the Commission on a daily basis. The "master" index version is the only 
    one with a delimiter. This function creates a list of url links to the master
    index file for each day in a given year or years with the prior_years argument.
    
    Args:
        year (int): Optional; The ending year for the list of links. Defaults to current
            year.
        prior_years (int): Optional; Number of years to return prior to current or given
            year. Default is None.
            
    Returns:
        A list of links to all of the daily master index files for the year(s)
        specified.
        
    Raises:
        TypeError: Arguments must be integers.
        ValueError: year argument must be 1994 or greater
        ValueError: Request precedes archive date limit (1994).
        ValueError: prior_years argument must be greater than zero
    """ 
    years = []
    if not isinstance(year, int):
        raise TypeError("Arguments must be integers.")
    if year < 1994:
        raise ValueError("year argument must be 1994 or greater")
    if prior_years is not None:
        if year - 1994 < prior_years:
            raise ValueError("Request precedes archive date limit (1994).")
        elif prior_years > 0:
            years = [year - i for i in range(prior_years + 1)]
            years.reverse()
        elif prior_years < 0:
            raise ValueError("prior_years argument must be greater than zero")
    else:  
        years = [year] 
    
    base_url = r"https://www.sec.gov/Archives/edgar/daily-index"
    master_idx_list = []
    
    for y in years:
        year_url = make_url(base_url, 
                            [y, 'index.json']
                           )
        content = requests.get(year_url).json()

        for item in content['directory']['item'][0:4]:
            qtr_url = make_url(base_url, 
                               [y, item['name'], 'index.json']
                              )
            qtr_content = requests.get(qtr_url).json()
            
            for file in qtr_content['directory']['item'][0:]:
                if "master" in file['name']:
                    file_url = make_url(base_url, 
                                        [y, item['name'], file['name']]
                                       )
                    master_idx_list.append(file_url)
                    
            speed_bump()
        
        speed_bump()
    
    return master_idx_list

def sql_path(yaml_path=None, login_key='login', user_key='username',
             pass_key='password', api_key=None, dialect=None,
             driver='default', host='localhost', port=None, database=None):
    """Returns a SQLAlchemy connection string from a yaml file.
    
    Based on the Engine Configuration guidelines in the SQLAlchemy docs.
    https://docs.sqlalchemy.org/en/14/core/engines.html
    
    Args:
        yaml_path (str): Location path for the yaml file containing keys
            associated with the inputs for a connection string. 
            Defaults to None.
        login_key (str): Location key in yaml file for username and
            password keys. Defaults to 'login'.
        user_key (str): YAML key containing a username. Defaults to
            'username'.
        pass_key (str): YAML key containing a password for the username
            given; will be encoded within function. Defaults to 
            'password'.
        api_key (str): Optional; Location key in yaml file for SQLAlchemy
            connection string inputs. If a string is passed, the function
            will first look in the yaml file for string inputs. Defaults
            to None.
        dialect (str): Optional; Dialect to use in connection string. 
            Defaults to None.
        driver (str): Optional; Driver to use in connection string. 
            If 'default' is passed, connection string will not use a
            driver in the string. Defaults to 'default'.
        host (str): Optional; Host to use in connection string. Defaults
            to 'localhost'.
        port (str): Optional; Port to use in connection string. If None
            is passed, the port will not be included in the string. 
            Defaults to None.
        database (str): Optional; SQL database to use in connection
            string. Defaults to None.
            
    Returns:
        A connection string to be used in SQLAlchemy's create_engine() 
        function.
        
    Raises:
        ValueError: No yaml path given for yaml_path argument.
        ValueError: No dialect given for dialect argument.
    """ 
    
    if yaml_path == None:
        raise ValueError('No yaml path given for yaml_path argument.')
    
    with open(yaml_path, 'r') as file:
        sql_yaml = yaml.load(file, 
                             Loader=yaml.FullLoader
                            )
    user = sql_yaml[login_key][user_key]
    password = sql_yaml[login_key][pass_key]
    pw_encoded = urllib.parse.quote_plus(password)
    
    if api_key is not None:
        dialect = sql_yaml[api_key].get('dialect', dialect)
        driver = sql_yaml[api_key].get('driver', driver)
        host = sql_yaml[api_key].get('host', host)
        port = sql_yaml[api_key].get('port', port)
        database = sql_yaml[api_key].get('database', database)
        
    if dialect == None:
        raise ValueError('No dialect given for dialect argument.')

    if driver == 'default':
        sql_path = f'{dialect}://{user}:{pw_encoded}@{host}/{database}'    
    elif port is None:
        sql_path = f'{dialect}+{driver}://{user}:{pw_encoded}@{host}/{database}'
    else:
        sql_path = f'{dialect}+{driver}://{user}:{pw_encoded}@{host}:{port}/{database}'
    
    return sql_path

def sql_dates(path, table=None, index=None, column=None,
              year=datetime.date.today().year, prior_years=None,
              yaml_path=None, api_key=None):
    """Returns a list of links for SEC master index files.
    
    Utilizes the pull_link_list function to generate a list of
    daily SEC master index files for year(s) given. Example
    source: r'https://www.sec.gov/Archives/edgar/daily-index
    /2020/QTR4/'. Function then pulls in SQL table where
    processed dates are stored and compares the list generated
    by the pull_link_list function. A list of dates that are
    yet to be processed is then returned.
    
    Args:
        path (str): Connection string to use in SQLAlchemy
            create_engine() function.
        table (str): Optional; SQL table of previously processed 
            master index file dates used for comparison. Defaults
            to None.
        index (str): Optional; Index column of table. Defaults to
            None.
        column (str): Optional; Table column with links of previously
            processed master index files. Defaults to None.
        year (int): Optional; Ending year of SEC master index file
            search (see pull_link_list() docstring). Defaults to
            current year.
        prior_years (int): Optional; Number of years to include in 
            master index file query. Default is None.
        yaml_path (str): Optional; Location path for yaml file
            containing keys associated with SQL inputs. Defaults
            to None.
        api_key (str): Optional; Location key in for yaml file
            for table, index, and/or column inputs. Defaults to
            None.
            
        Returns:
            List of unprocessed daily master index file links from
            the SEC website or message stating the SQL database is
            up to date.
            
        Raises:
            ValueError: No yaml path given for yaml_path argument.
        """
    
    if yaml_path == None:
        raise ValueError('No yaml path given for yaml_path argument.')
        
    master_idx_list = pull_link_list(year=year, 
                                     prior_years=prior_years
                                    )
    
    with open(yaml_path, 'r') as file:
        sql_yaml = yaml.load(file, 
                             Loader=yaml.FullLoader
                            )
    if api_key is not None:
        table = sql_yaml[api_key].get('table', table)
        index = sql_yaml[api_key].get('index', index)
        column = sql_yaml[api_key].get('column', column)
    
    engine = sql.create_engine(path)
    connection = engine.connect()
    
    if not connection.dialect.has_table(connection, table):
        meta = sql.MetaData()
        datetable = Table(table, meta,
                          Column('Date',DateTime,
                                 unique=True,nullable=False,index=True),
                          Column('Link',VARCHAR(255),
                                 unique=True,nullable=False)
                         )
        datetable.create(engine)
        print(f'{table} SQL table created.')
    else:
        date_sql = pd.read_sql_table(table, 
                                     con=connection, 
                                     index_col=index
                                    )
        new_dates = []
        for link in master_idx_list:
            if link not in date_sql[column].values:
                new_dates.append(link)
        if len(new_dates) >= 1:
            return new_dates
        else:
            print('SQL database is up to date.')

def parse_links(dates):
    """Returns a pandas DataFrame of filed SEC forms.
    
    Extracts information on all of the filed forms listed
    on SEC daily master index files. It accepts a list of
    links to these index files as its input. This list can 
    be generated using the pull_link_list function or 
    compared against a SQL database using the sql_dates 
    function, resulting in only unprocessed dates.
    
    Args:
        dates (list): List of links to daily SEC master
            index files
            
        Returns:
            DataFrame containing information on all of the
            individual forms filed based on the file links
            provided.
            
        Raises:
            TypeError: dates must be a list type.
    """
    if not isinstance(dates, list):
        raise TypeError('dates must be a list type.')
        
    idx_dfs = []
    for k in dates:
        content = requests.get(k).text
        names = ['CIK_int',
                 'comp_name',
                 'form_type',
                 'date_filed',
                 'file_name',
                ]
        start_index = re.search('CIK', content).start()
        df = pd.read_csv(StringIO(content[start_index:]),
                         sep='|',
                         skiprows=[1], 
                         names=names, 
                         header=0,
                         parse_dates=['date_filed']
                        )
        df['link'] = "https://www.sec.gov/Archives/" + df['file_name'].str.\
            replace("-", "").str.replace(".txt", "/index.json")
        idx_dfs.append(df)
        speed_bump()
    
    all_forms = pd.concat(idx_dfs, 
                          axis = 0, 
                          ignore_index = True
                         )
    print(f'Parsed index links for {len(idx_dfs)} dates.')
    return all_forms

def xml_list(df, form, form_col='form_type', link_col='link'):
    """Returns a list of SEC xml links for file name extraction.
    
    Produces a list of links from a column in the DataFrame
    given filtered by the form type provided. This list will
    be use to extract the proper xml path for the file(s)
    associated with the form type.
    
    Args:
        df (pandas DataFrame): DataFrame containing a
            column with xml links to an individual report.
        form (str or list): Form type(s) to use as filter(s).
        form_col (str): The DataFrame column used for form type
            filtering. Defaults to 'form_type'.
        link_col (str): DataFrame column containing xml links
        to individual reports. Defaults to 'link'.
        
    Returns:
        List of xml links to individual SEC reports.
        
    Raises:
        TypeError: df must be pandas DataFrame.
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError('df must be pandas DataFrame.')
        
    xml_list = df[df[form_col]==form][link_col].tolist()
    return xml_list

def xml_13f(json_list):
    """Returns two lists of Form 13F xml links.
    
    (Form Type: 13F-HR) Registered Investment Advisors a.k.a "Institutional
    Investors" have to disclose their company level holdings quarterly. 
    They do so using SEC form 13F. This form is in two pieces on the SEC 
    website. One is known as the "primary doc" and contains information on
    the filer and summary information. The second is the holdings file which
    contains the individual holdings. The primary doc is always named
    "primary_doc.xml", but the holdings file name varies. The function
    requests the json outputs from the list given and extracts the two file
    names and creates a link for them. There are a very small number of 
    filings that only have a primary doc and no holdings file. If the 13F
    is filed properly, a dictionary of the two links are appended to the 
    link_list. If the 13F does not have a holdings file, it is appended to
    the no_hold list.
    
    Assuming a parse_links() function call has been run, an xml_list()
    function call could produce the list required for input. Runtime is 
    about 120 links per minute.
    
    Args:
        json_list (list): List json links for individual SEC 13F forms.
        
    Returns:
        List of dicts containing the xml link for the primary doc and
        the holdings doc for each 13F filing.
        
        List of dicts containing the xml link to the primary doc of
        13f filings with no holdings file.
        
    Raises:
        TypeError: Argument must be a list type.
    """
    if not isinstance(json_list, list):
        raise TypeError('Argument must be a list type.')
    
    start_time = time.time()
    
    json_list = json_list
    link_list = []
    no_hold = []
    lcounter = 0
    
    for j in json_list:
        decode = requests.get(j).json()
        xml_dict = {}
        xml_count = 0
        for dic in decode['directory']['item'][0:]:
            for v in dic.values(): 
                if ".xml" in v.lower():
                    xml_count += 1
        for dic in decode['directory']['item'][0:]:
            if dic['name'] == "primary_doc.xml" and xml_count > 1:
                xml_dict['doc_xml'] = j.replace('index.json', '') + dic['name']
                xml_dict['doc_mod'] = dic['last-modified']
            elif ".xml" in dic['name'].lower() and dic['name'] != "primary_doc.xml":
                xml_dict['hold_xml'] = j.replace('index.json', '') + dic['name']
                xml_dict['hold_mod'] = dic['last-modified']
        if xml_dict:
            link_list.append(xml_dict)
        else:
            no_hold.append('https://www.sec.gov' + decode['directory']['name'])
        lcounter += 1
        if lcounter % 500 == 0:
            print(f'Processed {lcounter} 13F-HR form links.')

        speed_bump()

    print(f'\nFiles extracted, {((time.time() - start_time)/60):.2f} minutes.')
    print(str(f'Processed {len(json_list)} json links. '+
              f'{len(no_hold)} links have no holdings file.'))
    print('_'*50)

    return link_list, no_hold

def filers_13f(xml_list, key='doc_xml'):
    """Returns a pandas DataFrame with 13F filer information.
    
    Using a list of dictionaries, the function extracts information
    on the filing entity from a SEC Form 13F primary doc. The
    information extracted is independent of Form 13F and only
    contains contact and identification information.
    
    The first list returned from an xml_13f() call can produce
    the proper list for the xml_list argument.
    
    Args:
        xml_list (list): List of dictionaries containing xml links
            to the "primary doc" portion of a Form 13F filing.
        key (str): Dictionary key containing xml link to primary
            doc. Defaults to "doc_xml".
            
    Returns:
        A pandas DataFrame with 13F filer information.
        
    Raises:
        TypeError: xml_list must be a list type.
        TypeError: xml_list must be a list of dicts.    
    """
    if not isinstance(xml_list, list):
        raise TypeError('xml_list must be a list type.')
    elif not isinstance(xml_list[0], dict):
        raise TypeError('xml_list must be a list of dicts.')
        
    start_time = time.time()
    
    loop_no = 0
    append_list = []
    
    for dic in xml_list:
    
        dlink = dic[key]
        dreq = requests.get(dlink).content
        dsoup = BeautifulSoup(dreq, 'lxml')

        comp_dict = {}
        comp_dict['CIK'] = dsoup.find(re.compile('.*cik')).text
        for _fm in dsoup.find_all(re.compile('.*filingmanager')):
            comp_dict['company'] = _fm.find(re.compile('.*name')).text
            for _ad in _fm.find_all(re.compile('.*address')):
                comp_dict['street1'] = _ad.find(re.compile('.*street1')).text
                if _ad.find(re.compile('.*street2')) is not None:
                    comp_dict['street2'] = _ad.find(re.compile('.*street2')).text
                comp_dict['city'] = _ad.find(re.compile('.*city')).text
                comp_dict['stateorcountry'] = _ad.find(re.compile('.*stateorcountry')).text
                comp_dict['zipcode'] = _ad.find(re.compile('.*zipcode')).text

        if loop_no % 500 == 0:
            print(str(f'Extracting info on 13F filers, ' + 
                      f'{((time.time() - start_time)/60):.2f} minutes'))

        comp_df = pd.DataFrame([comp_dict])    
        append_list.append(comp_df)
        loop_no += 1
        speed_bump()

    full_df = pd.concat(append_list, 
                        axis = 0, 
                        ignore_index = True
                       )
    full_df.drop_duplicates(subset='CIK',
                            inplace=True,
                            ignore_index=True
                           )
    if 'street2' in full_df.columns:
        full_df = full_df[['CIK', 'company', 'street1', 'street2',
                          'city', 'stateorcountry', 'zipcode'
                         ]]
    print(f'\n{len(xml_list)} 13F-HR filers processed.')
    print('_'*50)
    
    return full_df

def file_info_13f(xml_list, doc_key='doc_xml', date_key='doc_mod'):
    """Returns a pandas DataFrame of data related to Form 13F filings.
    
    Using a list of dictionaries, the function extracts metadata on
    SEC Form 13F filings including file and filer IDs, period, totals,
    date filed, related manager filings, individual signature, etc.
    This information is extracted from the filing's primary doc as 
    well as the date modified (filing date) included in the file's
    json link.
    
    The first list returned from an xml_13f() call can produce
    the proper list for the xml_list argument
    
    Args:
        xml_list (list): List of dictionaries containing xml links
            to the "primary doc" portion of a Form 13F filing and
            timestamp information.
        doc_key (str): Dictionary key containing xml link to primary
            doc. Defaults to "doc_xml".
        date_key (str): Dictionary key containing timestamp of filing.
            Defaults to "doc_mod".
            
    Returns:
        A pandas DataFrame with information on individual 13F filings.
        
    Raises:
        TypeError: xml_list must be a list type.
        TypeError: xml_list must be a list of dicts.    
    """
    if not isinstance(xml_list, list):
        raise TypeError('xml_list must be a list type.')
    elif not isinstance(xml_list[0], dict):
        raise TypeError('xml_list must be a list of dicts.')

    start_time = time.time()
    
    loop_no = 0
    append_list = []
    
    for dic in xml_list:
        
        dlink = dic[doc_key]
        dreq = requests.get(dlink).content
        dsoup = BeautifulSoup(dreq, 'lxml')    

        comp_dict = {}
        
        comp_dict['CIK'] = dsoup.find(re.compile('.*cik')).text
        comp_dict['form'] = dsoup.find(re.compile('.*submissiontype')).text
        comp_dict['type'] = dsoup.find(re.compile('.*reporttype')).text
        comp_dict['date_filed'] = dic[date_key]
        if dsoup.find(re.compile('.*form13ffilenumber')):
            comp_dict['file_no'] =\
                dsoup.find(re.compile('.*form13ffilenumber')).text
        comp_dict['instruct5'] =\
            dsoup.find(re.compile('.*provideinfoforinstruction5')).text
        if dsoup.find(re.compile('.*provideinfoforinstruction5')).text == 'Y':
            comp_dict['instrc5info'] =\
                dsoup.find(re.compile('.*additionalinformation')).text
        comp_dict['period'] = dsoup.find(re.compile('.*periodofreport')).text
        comp_dict['quarter'] =\
            dsoup.find(re.compile('.*reportcalendarorquarter')).text
        if dsoup.find(re.compile('.*isamendment')):
            comp_dict['amend'] = dsoup.find(re.compile('.*isamendment')).text
        if dsoup.find(re.compile('.*othermanagersinfo')):
            othmgr = []
            for _om in dsoup.find(re.compile('.*othermanagersinfo')).children:
                if _om.name == re.compile('.*othermanager'):
                    omd = {}
                    if _om.find(re.compile('.*cik')):
                        omd['CIK'] = m.find(re.compile('.*cik')).text
                    if _om.find(re.compile('.*form13ffilenumber')):
                        omd['file_no'] =\
                            _om.find(re.compile('.*form13ffilenumber')).text
                    omd['name'] = _om.find(re.compile('.*name')).text
                    othmgr.append(omd)
            othmgr_json = [''.join(json.dumps(i)) for i in othmgr]
            othmgr_str = ' '.join(othmgr_json)
            comp_dict['oth_mgr'] = othmgr_str
        for _s in dsoup.find_all(re.compile('.*signatureblock')):
            sig = {}
            sig['name'] = _s.find(re.compile('.*name')).text
            sig['title'] = _s.find(re.compile('.*title')).text
            sig['phone'] = _s.find(re.compile('.*phone')).text
            sig['city'] = _s.find(re.compile('.*city')).text
            sig['stateorcountry'] = _s.find(re.compile('.*stateorcountry')).text
            sig['sig_date'] = _s.find(re.compile('.*signaturedate')).text
        comp_dict['signature'] = json.dumps(sig)
        if dsoup.find(re.compile('.*summarypage')):
            comp_dict['entry_total'] = dsoup.find(re.compile('.*tableentrytotal')).text
            comp_dict['value_total'] = dsoup.find(re.compile('.*tablevaluetotal')).text
            comp_dict['incld_mgrs'] =\
                dsoup.find(re.compile('.*otherincludedmanagerscount')).text
            if dsoup.find(re.compile('.*isconfidentialomitted')):
                comp_dict['confd_flag'] =\
                    dsoup.find(re.compile('.*isconfidentialomitted')).text
            if dsoup.find(re.compile('.*otherincludedmanagerscount')).text != '0':
                othmgr2 = []
                for _om2 in dsoup.find_all('.*othermanager2'):
                    imd ={}
                    imd['seq_no'] = _om2.find(re.compile('.*sequencenumber')).text
                    if _om2.find(re.compile('.*cik')):
                        imd['cik'] = _om2.find(re.compile('.*cik')).text
                    imd['name'] = _om2.find(re.compile('.*name')).text
                    if _om2.find(re.compile('.*form13ffilenumber')):
                        imd['file_no'] =\
                            _om2.find(re.compile('.*form13ffilenumber')).text
                    othmgr2.append(imd)
                othmgr2_json = [''.join(json.dumps(i)) for i in othmgr2]
                othmgr2_str = ' '.join(othmgr2_json)
                comp_dict['incl_mgr'] = othmgr2_str

        if loop_no % 500 == 0:
            print(f'Extracting 13F file info, {((time.time() - start_time)/60):.2f} minutes')
        
        comp_df = pd.DataFrame([comp_dict])
        comp_df[['period', 'quarter']] = comp_df[['period', 'quarter']]\
            .apply(pd.to_datetime, format = '%m-%d-%Y')
        comp_df[['date_filed']] = comp_df[['date_filed']].apply(pd.to_datetime)
        if dsoup.find(re.compile('.*summarypage')):
            comp_df[['value_total']] = comp_df[['value_total']].astype('float')
            comp_df[['entry_total','incld_mgrs']] =\
                comp_df[['entry_total','incld_mgrs']].astype('int')
        append_list.append(comp_df)
        loop_no += 1
        speed_bump()
        
    info_df = pd.concat(append_list, 
                        axis = 0, 
                        ignore_index = True
                       )
    info_df['file_id'] = info_df.CIK + info_df.file_no + info_df.period.astype('str')
    info_df['file_id'] = info_df['file_id'].str.replace('-','')
    print(f'\nInformation on {len(xml_list)} 13F-HR forms processed.')
    print('_'*50)
    
    return info_df

def holdings_13f(xml_list, 
                 hold_key='hold_xml',
                 doc_key='doc_xml',
                 date_key='hold_mod'
                 ):
    """Returns a pandas DataFrame of 13F holdings information.
    
    Using a list of dictionaries, the function extracts information
    on the individual holdings of individual Form 13F filings.
    This information is extracted from the filing's holding doc as 
    well as the date modified (filing date) included in the file's
    json link. It also uses some information from the filing's
    primary doc.
    
    The first list returned from an xml_13f() call can produce
    the proper list for the xml_list argument
    
    Args:
        xml_list (list): List of dictionaries containing xml links
            to both the "primary doc" and holdings doc portions
            of a Form 13F filing and timestamp information.
        hold_key (str): Dictionary key containing xml link to 
            holdings doc. Defaults to "hold_xml".
        doc_key (str): Dictionary key containing xml link to primary
            doc. Defaults to "doc_xml".
        date_key (str): Dictionary key containing timestamp of filing.
            Defaults to "hold_mod".
            
    Returns:
        A pandas DataFrame with information on the _holdings of 
        individual 13F filings.
        
    Raises:
        TypeError: xml_list must be a list type.
        TypeError: xml_list must be a list of dicts.    
    """
    if not isinstance(xml_list, list):
        raise TypeError('xml_list must be a list type.')
    elif not isinstance(xml_list[0], dict):
        raise TypeError('xml_list must be a list of dicts.')
    
    start_time = time.time()
    
    loop_no = 0
    hold_list = []
    append_list = []
    
    for dic in xml_list:
        
        hlink = dic[hold_key]
        hreq = requests.get(hlink).content
        hsoup = BeautifulSoup(hreq, 'lxml')
        for _holding in hsoup.find_all(re.compile('.*infotable')): 

            hold_dict = {}
            hold_dict['name'] = _holding.find(re.compile('.*nameofissuer')).text
            hold_dict['CUSIP'] = _holding.find(re.compile('.*cusip')).text
            hold_dict['class'] = _holding.find(re.compile('.*titleofclass')).text
            hold_dict['mkt_val'] = _holding.find(re.compile('.*value')).text
            hold_dict['shares'] = _holding.find(re.compile('.*sshprnamt')).text
            hold_dict['type'] = _holding.find(re.compile('.*sshprnamttype')).text
            if _holding.find(re.compile('.*putcall')):
                hold_dict['put_call'] = _holding.find(re.compile('.*putcall')).text
            hold_dict['discretion'] = _holding\
                .find(re.compile('.*investmentdiscretion')).text
            hold_dict['va_sole'] = _holding.find(re.compile('.*votingauthority'))\
                .find(re.compile('.*sole')).text
            hold_dict['va_shared'] = _holding.find(re.compile('.*votingauthority'))\
                .find(re.compile('.*shared')).text
            hold_dict['va_none'] = _holding.find(re.compile('.*votingauthority'))\
                .find(re.compile('.*none')).text
            if _holding.find(re.compile('.*othermanager')):
                hold_dict['othmgrdisc'] = _holding.find(re.compile('.*othermanager')).text
            
            hold_list.append(hold_dict)
            
        speed_bump()
        
        dlink = dic[doc_key]
        dreq = requests.get(dlink).content
        dsoup = BeautifulSoup(dreq, 'lxml')    

        comp_dict = {}
        comp_dict['CIK'] = dsoup.find(re.compile('.*cik')).text
        comp_dict['form'] = dsoup.find(re.compile('.*submissiontype')).text
        comp_dict['period'] = dsoup.find(re.compile('.*periodofreport')).text
        comp_dict['file_no'] = dsoup.find(re.compile('.*form13ffilenumber')).text
        comp_dict['date_filed'] = dic[date_key]
    
        if loop_no % 500 == 0:
            print(str(f'Extracting 13F holdings, ' +
                      f'{((time.time() - start_time)/60):.2f} minutes'))

        hold_df = pd.DataFrame(hold_list)
        hold_cols = ['mkt_val', 'shares', 'va_sole', 'va_shared', 'va_none'] 
        hold_df[hold_cols] = hold_df[hold_cols].astype('float')
        comp_df = pd.DataFrame([comp_dict])
        
        comp_df[['period']] = comp_df[['period']].apply(pd.to_datetime, 
                                                        format = '%m-%d-%Y'
                                                       )
        comp_df[['date_filed']] = comp_df[['date_filed']].apply(pd.to_datetime)
        stitch_df = pd.concat([comp_df,hold_df], 
                              axis = 1
                             ).ffill()
        append_list.append(stitch_df)
        hold_list = []
        loop_no += 1
        speed_bump()

    full_df = pd.concat(append_list, 
                        axis = 0, 
                        ignore_index = True
                       )
    if 'othmgrdisc' in full_df.columns:
        full_df[['othmgrdisc']] = full_df[['othmgrdisc']].apply(pd.to_numeric, 
                                                                errors='coerce'
                                                               )
    full_df['hold_id'] = full_df.CIK + full_df.file_no + full_df.period.astype('str')\
        + ':' + full_df.CUSIP
    full_df['hold_id'] = full_df['hold_id'].str.replace('-','')
    print(f'\n{len(xml_list)} 13F-HR holding files processed.')
    print('_'*50)
    
    return full_df

def sql_13f(path, table, df, id_col=None,
           typeset = {'CIK':Text,
                      'form':Text,
                      'type':Text,
                      'date_filed':DateTime,
                      'file_no':Text,
                      'period':Date,
                      'quarter':Date,
                      'signature':Text,
                      'entry_total':Integer,
                      'value_total':Float,
                      'incld_mgrs':Integer,
                      'confd_flag':Text,
                      'amend':Text,
                      'instruct5':Text,
                      'instrc5info':Text,
                      'oth_mgr':Text,
                      'incl_mgr':Text,
                      'file_id':Text,
                      'name':Text,
                      'CUSIP':Text,
                      'class':Text,
                      'mkt_val':Float,
                      'shares':Float,
                      'type':Text,
                      'discretion':Text,
                      'va_sole':Float,
                      'va_shared':Float,
                      'va_none':Float,
                      'put_call': Text,
                      'othmgrdisc':Text,
                      'hold_id':Text,
                      'company':Text,
                      'street1':Text,
                      'street2':Text,
                      'city':Text,
                      'stateorcountry':Text,
                      'zipcode':Text,
                      'CIK_int':Integer,
                      'comp_name':Text,
                      'form_type':Text,
                      'file_name':Text,
                      'link':Text
                     }
                ):
    """Uploads Form 13F DataFrames to SQL.
    
    Utilizing SQLAlchemy, the function creates or updates SQL tables using
    pandas DataFrames containing information on daily SEC filings generally
    and Form 13F filings specifically.
    
    Args:
        path (str): Connection string to use in SQLAlchemy
            create_engine() function.
        table (str): SQL table to be updated.
        df (pandas DataFrame): DataFrame to use to update SQL table.
        id_col (str): DataFrame column to compare to primary key of SQL
            table. Defaults to None.
        typeset (dict): Dictionary of column names and SQLAlchemy types.
            Default dictionary contains all of the columns produced by
            the parse_links(), filer_13f(), file_info_13f(), and
            holdings_13f() function calls. A list comprehension
            produces the proper list depending on the DataFrame.
    
    Raises:
        TypeError: df argument must be a pandas DataFrame.
        ValueError: No id column (primary key) found.
    
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError('df argument must be a pandas DataFrame.')
    if id_col == None:
        raise ValueError('No id column (primary key) found.')
        
    engine = sql.create_engine(path)
    connection = engine.connect()
    
    sqltypes = {key:value for (key, value) in typeset.items() if key in df.columns}

    # Check if tables exists, if true create table, if false append
    if not connection.dialect.has_table(connection, table):
        df.to_sql(table, 
                  con = connection, 
                  if_exists = 'fail', 
                  index = True, 
                  dtype = sqltypes)    
    else:
        exist_df = pd.read_sql_table(table, con=connection, columns=[id_col])
        exist_hold_id = exist_df[id_col].to_list()
        upload = df[~df[id_col].isin(exist_hold_id)]
        upload.to_sql(table, 
                      con = connection, 
                      if_exists = 'append', 
                      index = True, 
                      dtype = sqltypes)
        
    print(f"SQL {table} table updated.")

def sql_idx_dates(path, table, dates):
    """Creates or updates a SQL table of processed daily index files.
    
    Takes a list of links with date information and updates a SQL
    table with the dates associated with those links. The table
    updated in this function is intended to keep track of what
    daily master index files have been parsed from the SEC website.
    The intent is to update the table the sql_dates() functions
    utilizing when comparing available dates to processed dates.
    
    Args:
        path (str): Connection string to use in SQLAlchemy
            create_engine() function.
        table (str): SQL table to be updated.
        dates (list): List of SEC daily master index file
            links with date information.
            
    Raises:
        TypeError: dates argument must be a list type.
    """
    if not isinstance(dates, list):
        raise TypeError('dates argument must be a list type.')
    
    mdate_list = []
    for m in dates:
        datetuple = []
        mdate = m.split('/')[-1].split('.')[1]
        mdate_dt = datetime.datetime.strptime(mdate, "%Y%m%d")
        datetuple = [m, mdate_dt]
        mdate_list.append(datetuple)
    idx_df = pd.DataFrame(mdate_list, columns = ['Link', 'Date']).set_index('Date')
    
    engine = sql.create_engine(path)
    connection = engine.connect()

    # Check if tables exists, if true create table, if false append
    if not connection.dialect.has_table(connection, table):
        idx_df.to_sql(table, 
                  con = connection, 
                  if_exists = 'fail', 
                  index = True,
                  index_label = 'Date',
                  dtype = {'Link':Text,
                           'Date':Date
                          }
                     )
    else:
        exist_df = pd.read_sql_table(table, con=connection, columns=['Link'])
        exist_hold_id = exist_df['Link'].to_list()
        upload = idx_df[~idx_df['Link'].isin(exist_hold_id)]
        upload.to_sql(table, 
                      con = connection, 
                      if_exists = 'append', 
                      index = True, 
                      dtype = {'Link':VARCHAR,
                               'Date':Date
                              }
                     )
    
    print(f'SQL {table} dates table updated.')