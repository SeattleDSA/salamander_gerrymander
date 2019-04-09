# -*- coding: utf-8 -*-

#@title Imports and authorize
import urllib, urllib2, json, sys, csv, requests
import StringIO

#@title Get member data
#prev_sheet = "Seattle WA through 3-14-19" #@param {type:"string"}
#worksheet = gc.open(prev_sheet).worksheet('geolocated')
#rows = worksheet.get_all_values()

rows = None

input_csv = sys.argv[1]

with open(input_csv) as csvfile:
   reader = csv.reader(csvfile)
   rows = [row for row in reader]

headers = rows[0]
header_to_column_mapping = {index: name for index, name in enumerate(headers)}

def toDict(row):
  return { header_to_column_mapping[i]: value for i, value in enumerate(row) }

records = [toDict(row) for row in rows[1:]]

#@title geo boundaries
district_tracts = {
    'd1': ['009600', '009701', '009702', '009800', '009900', '010500', '010600', '010701', '010702', '010800', '011200', '011300', '011401', '011402', '011500', '011600', '012000', '012100', '026400', '026500'],
    'd2': ['009100', '009300', '010001', '010002', '010100', '010200', '010300', '010401', '010402', '010900', '011001', '011101', '011102', '011700', '011800', '011900', '026001'],
    'd3': ['006200', '006300', '006400', '006500', '007401', '007402', '007500', '007600', '007700', '007800', '007900', '008400', '008500', '008600', '008700', '008800', '008900', '009000'],
    'd4': ['002400', '002500', '002600', '003800', '003900', '004000', '004100', '004200', '004301', '004302', '004400', '005000', '005100', '005200', '005301', '005302', '005400'],
    'd5': ['000100', '000200', '000300', '000401', '000402', '000500', '000600', '000700', '000800', '000900', '001000', '001100', '001200', '001300', '001800', '001900', '002000', '002100'],
    'd6': ['001500', '001600', '001702', '002800', '002900', '003000', '003100', '003200', '003300', '003400', '003500', '004600', '004700', '004800', '004900'],
    'd7': ['005600', '005700', '005801', '005802', '005900', '006000', '006700', '006800', '006900', '007000', '007100', '007200', '007300', '008001', '008002', '008100', '008200'],
}

eastside_zips = [
    '98004',
    '98005',
    '98006',
    '98007',
    '98008',
    '98011',
    '98012',
    '98014',
    '98019',
    '98020',
    '98021',
    '98024',
    '98026',
    '98027',
    '98028',
    '98029',
    '98033',
    '98034',
    '98036',
    '98039',
    '98043',
    '98052',
    '98053',
    '98065',
    '98072',
    '98074',
    '98075',
    '98077',
    '98272',
    '98296',
]

south_king_zips = [
    '98042',
    '98058',
    '98056',
    '98002',
    '98030',
    '98031',
    '98047',
    '98001',
    '98198',
    '98032',
    '98003',
    '98023',
    '98057',
    '98055',
    '98178',
    '98188',
    '98158',
    '98148',
    '98168',
    '98166',
    '98146',
    '98118',
    '98108',
    '98136',
    '98126',
    '98106',
    '98092',
    '98059',
]

north_king_zips = [
    '98177',
    '98133',
    '98155',
    '98125',
]

def getDistrict(tract, block_group, block):
  block_group = int(block_group)
  block = int(block)

  if tract in district_tracts['d1']:
    return 1
  if tract in district_tracts['d2']:
    return 2
  if tract in district_tracts['d3']:
    return 3
  if tract in district_tracts['d4']:
    return 4
  if tract in district_tracts['d5']:
    return 5
  if tract in district_tracts['d6']:
    return 6
  if tract in district_tracts['d7']:
    return 7

  # split tracts
  if tract == '001400':
    return 5 if block_group in [1,2] or block in range(4000,4005) + [4008] else 6
  if tract == '001701':
    return 6 if block_group == 1 else 5
  if tract == '002200':
    return 4 if block_group == 4 else 5
  if tract == '002700':
    return 4 if block_group == 1 or block in range(2001,2009) + range(2024,2028) else 6
  if tract == '003600':
    return 4 if block in range(1000,1034) + range(1045,1050) else 6
  if tract == '004500':
    return 4 if block_group == 1 else 6
  if tract == '006100':
    return 3 if block_group in range(1,3) else 4
  if tract == '006600':
    return 3 if block in range(1000,1024) + [1029,1031,1032] + range(1035, 1046) else 4
  if tract == '008300':
    return 3 if block_group == 1 else 7
  if tract == '009200':
    return 2 if block_group == 1 else 7
  if tract == '009400':
    return 3 if block_group == 2 else 2
  if tract == '009500':
    return 2 if block_group in [3,4] else 3

  # These are white center, so not in any council district, otherwise unexpected
  if tract not in ['026801', '027000', '026600', '027500']:
    print 'Sad Times: could not find council district for tract {} block {}'.format(tract, block)

  return -1

def isEastsideZip(z):
  for eastside_zip in eastside_zips:
    if eastside_zip in z:
      return True
  return False

def isSouthKingZip(z):
  for south_king_zip in south_king_zips:
    if south_king_zip in z:
      return True
  return False

def isNorthKingZip(z, in_seattle):
  if in_seattle:
    return False
  for north_king_zip in north_king_zips:
    if north_king_zip in z:
      return True
  return False

#@title geolocation and data munging
def getEmptyAddressInfo():
  return {'geolocated_address': False, 'in_seattle': False, 'council_district': -1, 'is_eastside': False, 'is_south_king': False, 'is_north_king': False}


def getRowAddressInfo(row):
  ret = getEmptyAddressInfo()
  query_params = {}
  query_params['street'] = row['Address_Line_1']
  query_params['city'] = row['City']
  query_params['state'] = row['State']
  query_params['zip'] = row['Zip'][:5]
  querystring = urllib.urlencode(query_params)
  url = 'https://geocoding.geo.census.gov/geocoder/geographies/address?' + querystring + '&benchmark=Public_AR_Census2010&vintage=Census2010_Census2010&layers=14&format=json'
  response = urllib2.urlopen(url)
  response_html = response.read()
  response_json = json.loads(response_html)

  if len(response_json['result']['addressMatches']) == 0:
    return ret

  result = response_json['result']['addressMatches'][0]
 
  if len(result['geographies']['Census Blocks']) == 0:
    return ret
  
  census_block = result['geographies']['Census Blocks'][0]
         
  if result['addressComponents']['city'] == 'SEATTLE':
    ret['in_seattle'] = True
    ret['council_district'] = getDistrict(census_block['TRACT'], census_block['BLKGRP'], census_block['BLOCK'])

  if isEastsideZip(result['addressComponents']['zip']):
    ret['is_eastside'] = True

  if isSouthKingZip(result['addressComponents']['zip']):
    ret['is_south_king'] = True

  if isNorthKingZip(result['addressComponents']['zip'], ret['in_seattle']):
    ret['is_north_king'] = True

  ret['geolocated_address'] = True

  return ret

def addDistrictInfo(records):
  # Batch fetch.
  url = 'https://geocoding.geo.census.gov/geocoder/geographies/addressbatch'

  csv_addr = StringIO.StringIO()
  csv_writer = csv.writer(csv_addr, quoting=csv.QUOTE_ALL)

  addr_headers = ['Address_Line_1','City','State','Zip']
  
  for i, record in enumerate(records):
    new_record = [i]
    for attr in addr_headers:
      if attr == 'Zip':
        new_record.append(record[attr][:5])
      else:
        new_record.append(record[attr])
    csv_writer.writerow(new_record)
    
  resp = requests.post(url, files = {
    'addressFile': ('addresses.csv', csv_addr.getvalue().strip())
  }, data = {
    'benchmark': 'Public_AR_Census2010',
    'vintage': 'Census2010_Census2010',
  })

  csv_addr.close()
  
  reader = csv.reader(resp.iter_lines())

  new_records = [record for record in records]

  # Parse response.
  for i, row in enumerate(reader):
    ret = getEmptyAddressInfo()

    # Census fetch successful
    if row[2] == 'Match':
      if 'SEATTLE, WA' in row[4]:
        ret['in_seattle'] = True
        ret['council_district'] = getDistrict(row[-2], row[-1][0], row[-1])
        
      z = row[4][-5:]
      
      if isEastsideZip(z):
        ret['is_eastside'] = True
      if isSouthKingZip(z):
        ret['is_south_king'] = True
      if isNorthKingZip(z, ret['in_seattle']):
        ret['is_north_king'] = True
      ret['geolocated_address'] = True
    # Census fetch returned 2+ equally "likely" canonical addresses. Fallback to single-row fetch.
    elif row[2] == 'Tie':
      ret = getRowAddressInfo(records[i])
      
    for attr, value in ret.iteritems():
      new_records[i][attr] = value
      
  return new_records, addr_headers

def combineDfsByIndex(host, guest):
  for irow in guest.iterrows():
    index = irow[0]
    row = irow[1]
    for col in row.items():
      col_name = col[0]
      col_val = col[1]
      host.loc[index,col_name] = col_val

records_with_geocoding, addr_headers = addDistrictInfo(records)

output_csv = input_csv.replace(".csv", " with districts.csv")

with open(output_csv, 'wb') as output:
  output_writer = csv.writer(output)
  output_writer.writerow(headers + addr_headers)
  for record in records_with_geocoding:
    values = [record[header] for header in headers]
    output_writer.writerow(values)


#files.download('with_districts.csv')
# heres where we want to output combined_ef as a csv

"""## City Council Districts - Census Tract splits

### d1
026400: 4001-4003,5008
026500: 1000,1002-1003,1009,5008
### d2
009200: 1
009400: 1,3-5
009500: 3-4
026001: 1006,1008,1023,1026-1028,1031-1032
### d3
006100: 1-2
006600: 1000-1023,1029,1031-1032,1035-1045
008300: 1
009400: 2
009500: 1-2
### d4
002200: 4
002700: 1,2001-2008,2024-2027
003600: 1000-1033, 1045-1049
004500: 1
006100: 3-4
006600: 2,1024-1028,1030,1033-1034
### d5
001400: 1-2,4000-4004,4008
001701: 2-3
002200: 1-3
### d6
001400: 3,4005-4007,4009-4021
001700: 1
002700: 3-4,2009-2023,2028-2031
003600: 2-4,1034-1044
004500: 2
### d7
008300: 2
009200: 2

## Eastside Zips

98004	Bellevue
98005	Bellevue
98006	Bellevue
98007	Bellevue
98008	Bellevue
98011	Bothell
98012	Mill Creek
98014	Carnation
98019	Duvall
98020	Edmonds
98021	Canyon Park
98024	Fall City
98026	Edmonds
98027	Issaquah
98028	Kenmore
98029	Issaquah
98033	Kirkland
98034	Kirkland
98036	Lynnwood
98039	Medina
98043	Mountlake Terrace
98052	Redmond
98053	Redmond
98065	Snoqualmie
98072	Woodinville
98074	Sammamish
98075	Sammamish
98077	Woodinville
98272	Monroe
98296	Maltby
"""
