import csv
import pandas as pd
import optparse
import time
import datetime
from google.cloud import bigquery
import warnings
warnings.filterwarnings("ignore", "Your application has authenticated using end user credentials")

# parsing r2 .csv's
def task():
    df = pd.read_csv('r2.csv',sep='\t',error_bad_lines=False)
    print(df)
    array = ['UMGOWN','EXCLIC','JOINTV']
    df = df.loc[df['Rights Type Id'].isin(array)]
    df.to_csv('r2_filter.csv')

def get_user_tags(user):
    query_string = """
        SELECT distinct t2.isrc as isrc, t1.tag_guid as guid, t2.value as value
        FROM `umg-alpha.ircam.amplify_user_activity` as t1
        INNER JOIN `umg-edw.metadata.amplify_tem_v3_3` as t2
        ON t1.entity_id = t2.tagged_entity_id
        WHERE t1.tag_added_by = {};
    """.format("'"+user+"'")
    bqc = bigquery.Client()
    results = bqc.query(query_string).result()
    # print(results)
    df = results.to_dataframe()
    # print(df)
    return df

def get_num_combos(g):
    #g is number of guids
    return (int((g**2-g)/2))

def get_from_bq(isrcs):
    print("Fetching Required Data")
    query_string = """
            SELECT distinct t1.isrc as isrc,t1.taxonomy_node_id as guid, t1.path as path, t1.value as value
            FROM `umg-edw.metadata.amplify_tem_v3_3`AS t1
            WHERE t1.source = \'Manual\'
            AND t1.isrc in ({});
    """.format(", ".join(repr(e)[1:-1] for e in isrcs))
    bqc = bigquery.Client()
    results = bqc.query(query_string).result()
    df = results.to_dataframe()
    df = df.drop_duplicates()
    return df

def parse_audit_table():
    df = pd.read_csv('updated_combinations.csv')
    return df

def combinations(guids):
    combos = []
    for i in range(len(guids)):
        for j in range (i+1,len(guids)):
            combos.append((guids[i],guids[j]))
    return combos

#Get the value/name of a guid
#passes df of ['guid','value']
def getValue(guid, df):
    check = df.loc[df["guid"] == (guid)]
    check = check.drop_duplicates()
    check = check.reset_index()
    return check.at[0,'value']
                
def appendAux(output,aux,df,isrc):
    for idx, row in aux.iterrows():
        ave  = (row["VALUE"] / row["COMBOS"])
        value = getValue(row["GUID"],df)
        output = output.append({'ISRC':isrc,'TAG':value,'AVERAGE':ave},ignore_index=True)
    return output

def checkPair(pair,table):
    guid1 = pair[0]
    guid2 = pair[1]
    check = table.copy(deep = True)
    check = check.loc[check['Tag1'] == (guid1)]
    check = check.reset_index()
    check = check.loc[check['Tag2'] == (guid2)]
    check = check.reset_index()
    value = -1
    miss = 0
    if not check.empty:
        value = check.at[0,'Value']
    else:
        miss = 1
    return value,miss 

def addValues(value,pair,aux):
    guid1 = pair[0]
    guid2 = pair[1]
    r1 = aux.loc[aux['GUID'] == (guid1)]
    r2 = aux.loc[aux['GUID'] == (guid2)]
    if not r1.empty:
        aux.loc[aux['GUID'] == (guid1),'VALUE'] = r1['VALUE'] + value
        aux.loc[aux['GUID'] == (guid1),'COMBOS'] = r1['COMBOS'] + 1
    else:
        aux = aux.append({'GUID':guid1,'VALUE':value,'COMBOS':1},ignore_index=True)
    if not r2.empty:
        aux.loc[aux['GUID'] == (guid2),'VALUE'] = r2['VALUE'] + value
        aux.loc[aux['GUID'] == (guid2),'COMBOS'] = r2['COMBOS'] + 1
    else:
        aux = aux.append({'GUID':guid2,'VALUE':value,'COMBOS':1},ignore_index=True)
    return aux

table = parse_audit_table()
p = optparse.OptionParser()
p.add_option('--file','-f', dest="filename", help = "run input on .csv file (Amplify export or column of ISRCs)")
p.add_option('--user','-u', dest="username", help = "analyze all UMG user tag information from BigQuery")
p.add_option('--output','-o',dest="output",default = "AuditReport", help = "write audit report to FILENAME.csv, otherwise will default to AuditReport(Timestamp).csv")
p.add_option('--combinations','-c',action="store_true",dest="combinations", help="Audit and output all possible combination analytics per ISRC")
p.add_option('--impossible','-i',action="store_true", dest="impossible",help="Output all impossible tag combinations (if any exist)")
p.add_option('--quiet','-q',action="store_true",dest="verbose",help="don't print status messages to stdout")
options, arguments = p.parse_args()
if options:
    if options.filename:
        df = pd.read_csv(options.filename)
        df = df.drop_duplicates()
        if len(df.columns) == 1 or len(df.columns) == 2:
            df = get_from_bq(df.values.tolist())
        elif len(df.columns) == 4:
            print("Parsing Input File")
        elif len(df.columns) > 3:
            #Amplify Export Files
            try:
                sub_df = df[['entityId (e.g ISRC)']]
            except:
                sub_df = df[['isrc']]
            sub_df = sub_df.drop_duplicates()
            df = get_from_bq(sub_df.values.tolist())
            # print(df)
            # exit(1)
        #df holds isrc, guid, path, value
    elif options.username:
        df = get_user_tags(options.username)
    else:
        print("Options:")
        print("-h, --help \t show this help message and exit")
        print("-o FILENAME, --output=FILENAME \t write audit report to FILENAME.csv, otherwise will default to Audit(Timestamp).csv")
        print("-q, --quiet \t don't print status messages to stdout")
        print("______________________________________________________")
        print("Input File (only one):")
        print("-f FILE, --file=FILE \t run input on .csv file (Amplify export or column of ISRCs)")
        print("-u EMAIL, --user=EMAIL \t analyze all UMG user tag information from BigQuery")
        print("______________________________________________________")
        print("Optional Flag (only one):")
        print("-c, --combinations \t Audit and output all possible combination analytics per ISRC")
        print("-i, --impossible \t Output all impossible tag combinations (if any exist)")     
        exit(1)
    isrcs_guids = df[['isrc','guid','value']]
    #get number of isrcs
    num_isrc = isrcs_guids['isrc'].nunique()
    #get unique isrcs
    isrc_list = isrcs_guids.isrc.unique()
    if options.impossible:
        output = pd.DataFrame(columns = ['ISRC','COMBOS'])
    elif options.combinations:
        output = pd.DataFrame(columns = ['ISRC','COMBOS','VALUE'])
    else:
        output = pd.DataFrame(columns = ['ISRC','TAG','AVERAGE'])

    for i in range(num_isrc):
        #have isrc with all its guids
        #curr is a list of all guids under the ith ISRC
        curr = isrcs_guids.loc[isrcs_guids['isrc'] == isrc_list[i]].copy(deep = True)
        curr.reset_index()
        num_guids = curr['guid'].nunique()
        num_combos = get_num_combos(num_guids)
        #iterate through guids, and map all valid combinations, sum up all values from table
        guids = curr.guid.unique()
        guid_combos = combinations(guids)
        aux = pd.DataFrame(columns = ['GUID','VALUE','COMBOS'])
        if options.impossible:
            sub_table = df[['guid','value']]
            lst = [] #for bad combinations
            for pair in guid_combos:
                value = 0
                ret1, ret2 = checkPair(pair,table)
                if ret1 == 0:
                    # lst.append((getValue(pair[0],sub_table),getValue(pair[1],sub_table)))
                    output = output.append({'ISRC':isrc_list[i],'COMBOS':(getValue(pair[0],sub_table),getValue(pair[1],sub_table))},ignore_index=True)
        elif options.combinations:
            sub_table = df[['guid','value']]
            for pair in guid_combos:
                value = 0
                ret1, ret2 = checkPair(pair,table)
                if ret2 != 1:
                    value = ret1
                    output = output.append({'ISRC':isrc_list[i],'COMBOS':(getValue(pair[0],sub_table),getValue(pair[1],sub_table)),'VALUE':value},ignore_index=True)
        else:
            for pair in guid_combos:
                value = 0
                ret1, ret2 = checkPair(pair,table)
                if ret2 != 1:
                    value = ret1
                    #add values to each pair in aux
                    aux = addValues(value,pair,aux)

            output = appendAux(output,aux,df[['guid','value']],isrc_list[i])
        if not options.verbose:
            print(str(i+1)+"/"+str(num_isrc)+" ISRCS analyzed")
    timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M')
    if options.output == 'AuditReport':    
        output.to_csv(str(options.output)+'('+timestamp +')'+'.csv')
    else:
        output.to_csv(str(options.output)+'.csv')
    if not options.verbose:
        print("Audit Complete")