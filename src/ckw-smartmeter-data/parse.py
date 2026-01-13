"""
  Parse CKW open data hosted at https://open.data.axpo.com

  There are two datasets provided by CKW, one of them is ...:
    **Datensatz A** : dataset a, anonymized data
    **Datensatz B** : dataset b, aggregated data

  This script does the following:
    1. Download data from website
    2. Unzip data
    3. Parse data to a compressed format

  The file names can be parsed from the website or a text file, see metatada.txt

  TODO: format of file names is given, just create file name with user input
          YYYYMM.csv.gzs

  TODO: evaluate streaming IO in py arrow
          import gzip
          with gzip.open('example.gz', 'wb') as f:
            f.write(b'some data\n' * 3)
          stream = pa.input_stream('example.gz')
          stream.read()

  *** other notes/todos ***
  # TODO: turn it into a command line script, main parameters include 
         * download directory
         * storage directory
      the application should simply, 
         (1) scan the available disk space
         (2) make sure directories are available and writeable
         (3) clean directories if needed
             - delete CSV files
             - delete gz files
         (4) parse *.csv files (if any) to *.parquet files 
             - clean *.csv files after parsing
         (5) unzip *.gz files that are already downloaded
             - clean *.gz files after
         (6) assess which of the files available for download are already 
         parsed to parquet
         (7) download and parse remaining files (clean up if needed)

  *Note: prepare the download directories before runnign the scripts
"""

import os
import glob
import requests
from bs4 import BeautifulSoup
import pandas as pd
import dask
dask.config.set({'dataframe.query-planning': True})
import dask.dataframe as dd
#import pyarrow as pa
#import pyarrow.csv
#import pyarrow.parquet as pq 

# convert metadata_parser to a class
class MetadataParser:
  def __init__(self):
    self.url_da = "https://open.data.axpo.com/%24web/index.html#dataset-a"
    self.html_content = requests.get(self.url_da).text
    self.soup = BeautifulSoup(self.html_content, features="html.parser")
    self.html_tables = self.soup.find_all("table")
    self.file_names = []
    self._prefix = "https://open.data.axpo.com/%24web/"

  def file_names(self, table = "dataset A"):
    """ TODO: write doc string :: get file names from files listed in CKW open 
    data portal
    """
    tables = {"dataset A": self.html_tables[0], 
              "dataset B": self.html_tables[1], 
              "renewable energy data": self.html_tables[2]
              }
    
    tr_list = tables[table].find_all("tr") # header and rows
    for tr_i in tr_list:
      fn = tr_i.find_all("th")[0].text

      if fn.endswith(".csv.gz"):
        self.file_names.append(fn)

        file_size = tr_i.find_all("td")[2].text
        date = tr_i.find_all("td")[1].text
        #link = tr_i.find_all("td")[-1].contents # link
      else:
        continue

    return self.file_names, self._prefix, self.html_table

  def __call__(self):
    """ TODO: test this method
    """
    return self.metadata_parser()

def metadata_parser():
  """ Parse website text to extract file names and other metadata  
  """
  url_da = "https://open.data.axpo.com/%24web/index.html#dataset-a"

  # get html content
  html_content = requests.get(url_da).text # string

  # parse html content, extract body from html content text
  soup = BeautifulSoup(html_content, features="html.parser")
  html_tables = soup.find_all("table") # ResultSet object : 
                                       #  
                                       # dataset a (anonymized), 
                                       # dataset b (aggregated),
                                       # *!* renewable energy data

  # *!* dataset a is the first table in the html content
  tr_list = html_tables[0].find_all("tr") # header and rows
  hdrs = tr_list[0].find_all("th") # header
  dic_table = {hdr.text: [] for hdr in hdrs}
  for row in tr_list[1:]:
    cells = row.find_all(["th", "td"])
    for cell, hdr in zip(cells, hdrs): 
      dic_table[hdr.text].append(cell.text)

  df= pd.DataFrame(dic_table)
  df[['DateigrÃ¶sse', 'Export Datum', 'Zeitraum', 'Dateiname']]         

  tr_list[1].find_all("td") # one row
  tr_list[1].find_all(["th", "td "]) # one row
  for row in tr_list:
    cells = row.find_all(["th", "td"])
    print(" | ".join([cell.get_text() for cell in cells]))

  # find in bs4.element.ResultSet
  file_names = []
  for tr_i in tr_list:
    fn = tr_i.find_all("th")[0].text

    if fn.endswith(".csv.gz"):
      file_names.append(fn)

      file_size = tr_i.find_all("td")[2].text
      date = tr_i.find_all("td")[1].text
      #link = tr_i.find_all("td")[-1].contents # link
    else:
      continue

  # construct download url
  _prefix = "https://open.data.axpo.com/%24web/"

  # get file name from user input or from a list
  #   fn0 = "ckw_opendata_smartmeter_dataset_a_202308.csv.gz" # for download
  #   url0 = "https://open.data.axpo.com/%24web/ckw_opendata_smartmeter_dataset_a_202308.csv.gz"

  return file_names, _prefix, html_table

def filenames():
  """ Description goes here ...
  """
  f2unzip = os.listdir(downloads)
  _gz_names = [i.split(".csv.gz")[0] for i in file_names] 

  # Compare available files in ckw opendata portal to files already parsed to parquet
  _fls_pq = [i for i in os.listdir(storage) if i.endswith(".parquet")]
  _fls_CSV = [i for i in os.listdir(storage) if i.endswith(".csv")]
  _pq_names = [i.split(".parquet")[0] for i in _fls_pq if i.startswith("ckw_opendata_smartmeter_dataset_a")]
  _gz_downloaded = [i.split(".csv.gz")[0] for i in os.listdir(downloads) if i.startswith("ckw_opendata_smartmeter_dataset_a")]
  f2parse = set.difference(set(_gz_names), set(_pq_names))

  _gz_downloaded.sort()
  _redundant_gz = set.intersection(set(_gz_downloaded), set(_pq_names))
  _redundant_csv = set.intersection(set(_pq_names), set(_fls_CSV))
  
  if True:
    print("====================")
    print("* {:} parquet files are saved at: {:}".format(len(_pq_names), storage))
    print("* {:} gzip files are downloaded at: {:}".format(len(_gz_downloaded), downloads))
    print("* {:} gzip files are available to download from: {:}".format(len(file_names), url_prefix))

    print("====================")
    if len(_redundant_gz) > 0:
      print("The followwing are somewhat redundant files at: {:}".format(downloads))
      _ = [print(i) for i in _redundant_gz]
      print("because they are already saved as parquet files at: {:}".format(storage))
    elif len(_redundant_gz) == 0:
      print("No redundant files were found at: ")
      print("               {:}".format(downloads))

    print("===================")
    if len(_redundant_csv) > 0:
      print("The followwing are somewhat redundant files at: {:}".format(storage))
      _ = [print(i) for i in _redundant_csv]
      print("because they are already saved as parquet files at: {:}".format(storage))
    elif len(_redundant_csv) == 0:
      print("No redundant files were found at: ")
      print("               {:}".format(storage))
    
    return _redundant_gz, _redundant_csv, _fls_pq, _fls_CSV, f2parse, f2unzip

def cleaning():
  # --- cleaning --- #
  for i in _redundant_gz:
    fi = os.path.join(downloads, i) + ".csv.gz"
    try:
      os.remove(fi)
    except:
      print("File {:} not found".format(fi))

def prepare_dirs():
  """ TODO: move to cleaning
  """
  if flg_rm_CSV:
    os.system("rm {:}/*.csv".format(storage))

def copy(dir_source, dir_target):
  #dir_source = "/home/ubuntu/storage/ckw/*"
  #dir_target = "/home/ubuntu/data/ckw/storage"
  fns = glob.glob(dir_source + "/*", dir_target)
  for i in fns:
    o = os.path.join(dir_target, 
    os.path.basename(i))
    
    #print("cp -rf {:} {:}".format(i, o))
    #os.mkdir(o)
    os.system("cp -rf {:} {:}".format(i, o))
    print(".")

def download(fn, url_prefix, flg="curl"):
  """
    fn : 
    url_prefix:

    TODO: break when no file is found to download
  """
  if os.path.exists(fn):
    print("This file already exists: {}".format(fn))
  elif not os.path.exists(fn):
    # build url to download file from
    url_i = url_prefix + os.path.basename(fn)
    
    # download file and save it to specified path fn
    curl_cmd = f"curl -o {fn} {url_i}"

    print(curl_cmd)
    _msg = os.system(curl_cmd)
  return _msg

def download_zenodo_record(zr, url="https://zenodo.org/api/records", flg="curl", **kwargs):
  """
    TODO: break when no file is found to download
  """
  if os.path.exists(fn):
    print("This file already exists: {}".format(fn))
  elif not os.path.exists(fn):
    # build url to download file from
    if url_zenodo:
      url_i = url_zenodo
      print(url_i)
    else:
      url_i = url_prefix + os.path.basename(fn)
    
    # download file and save it to specified path fn
    curl_cmd = f"curl -o {fn} {url_i}"

    print(curl_cmd)
    _msg = os.system(curl_cmd)
  return _msg

def gunzip(fn, fout, flg="gunzip"):
  if os.path.exists(fn):
    # TODO: address edge cases
    # TODO: there most be a python wrapper already providing a lot of control over the 
    #         arguments passed to gunzip or other tool, use flg to select tool
    print("Unzip {:} to {:}".format(fn, fout))    
    # unzip file
    unzip_cmd = f"gunzip -c {fn} > {fout}"
    print(unzip_cmd)
    _msg = os.system(unzip_cmd)
  return _msg

def batch_download():
  """ Download a batch of files
    1. given a local directory
    2. check if files exist
    3. try to download files from the website
  
    *!* Note: you need to have enough disk space, this naive approach will write empty files when
    there is not enough space, those you need to clean later
  """
  # check if files exist
  downloads = "/home/ubuntu/data/downloads/ckw" #"/Users/tabaraho/ckw" # *.gz and *.csv files
  for fi in file_names:
    fn = os.path.join(downloads, fi)
    _curl_output = download(fn)

  # Unzip files
  for fi in file_names:
    fn = os.path.join(downloads, fi)
    fout = os.path.join(downloads, fi.replace(".gz", ""))
    _gunzip_output = gunzip(fn, fout)

def csv2parquet(fi):
  """ Use Dask to parse CSV to parquet 
    fi : csv file name

    *!* TODO: use rather polars: https://h2oai.github.io/db-benchmark/
  """

  fi = os.path.join(storage, fi)
  fo = os.path.join(storage, os.path.basename(fi).replace("csv", "parquet"))

  if not os.path.exists(fo): # if there is no parquet file, then parse CSV to dataframe
                                        #  and write to Dask dataframe to parquet
    print("Parse {:} to dataframe".format(fi))

    # workflow: (1) use dask to read csv in blocks; 
    #           (2) write dask data frame to parquet
    ddf = dd.read_csv(fi, blocksize="64MB") # blocksize default is automatically 
                                                  # calculated or 64 MB
    ddf.partitions[0]

    print("Write dataframe to {:}".format(fo))
    ddf.to_parquet(fo) # *!* outputs a dataset
    # TODO: when writting out parquet file include _metadata

  elif os.path.exists(fo):
    print("{:} already exists".format(fo))

  """ TODOs: 
        * parse CSV and save to parquet directly with arrow library:
              table = pa.csv.read_csv(f_out) # *!* overflods memory
              pa.write_table(table, f_out.replace("csv", "parquet"))
  """

# ---- #
# Main #
# ---- #
if __name__ == "__main__":
  """ Parse gz to parquet
    1. unzip a file from data folder to storage folder
    2. read csv file to memory
    3. save pyarrow table to parquet

    TODO:  describe the setup of data storage
      * a folder to download the original data from CKW open data platform, csv files each one a few GBs
      
    TODO: check if output directory exists, if not create it
    TODO: check disk space before unzipping file
    TODO: check disk space before saving parquet file
    TODO: be aware if users cannot write to disk
  """
  # --- Parse settings --- #
  # Read file names in data folder
  downloads = "/home/ubuntu/data/ckw/downloads" #"/Users/tabaraho/ckw" # *.gz and *.csv files
  downloads = "/home/ubuntu/data/ckw/downloads" #"/Users/tabaraho/ckw" # *.gz and *.csv files
  
  storage = "/home/ubuntu/data/ckw/storage" #"/Users/tabaraho/ckw" # *.gz and *.csv files

  flg_rm_CSV = True # remove CSV files
  flg_rm_GZ = True # remove downloaded zip files

  file_names, url_prefix, _ = metadata_parser()
  _redundant_gz, _redundant_csv, _fls_pq, _fls_CSV, f2parse, f2unzip = filenames()

  # *!* clean if needed or wished, see cleaning() and prepare_dirs()

  # Unzip files that are already downloaded in the data folder to storage folder
  if len(f2unzip) > 0:
    files2unzip = []
    for f in f2unzip:
      if f.startswith("ckw_opendata_smartmeter_dataset_a") & f.endswith(".gz"):
        files2unzip.append(f)
        #print("File to unzip: {}".format(f))
      else:
        print("This file seems not to belong to the CKW dataset: {:}".format(f))

    files2unzip.sort()
    N = 8 #len(files2unzip) # TODO: expose it as an option, to not process all files at once
    for i in range(N): #fns: # *!* maybe sort of prompt the user to enter the file name
      """ Monolith workflow
        1. get list of *gz files
        2. parse to arrow, 
        3. get ids,
        4. save id file
        5. save data file
      """
      fn = files2unzip[i]
      if fn.endswith(".gz"): # *!* redundant check
        f_in = os.path.join(downloads, fn)
        f_out = os.path.join(storage, fn.replace(".gz", "")) #

        unzip_cmd = f"gunzip -c {f_in} > {f_out}" # unzip f_in and output csv file f_out

        try:
          if not os.path.exists(f_out):
            print("The following file will be unzipped: ") # parsing message
            print(unzip_cmd)
            os.system(unzip_cmd)
        except: # TODO: catch and handle exceptions
          pass

        # TODO: log files that were unzipped, use logging module or somethin else to avoid cluttering 
        # the console with messages about unzipping files that were already unzipped, and the main
        # code base with print statements
        if os.path.exists(f_out):
          print("Unzipped *.gz to: {}".format(os.path.basename(f_out)))

          if True: # TODO: expose it for user to control if they want to delete *.gz files
            print("I am deleting: {:}".format(f_in))
            os.remove(f_in)
          # TODO: write to log file
          #with open("unzipped_files.log", "a") as f:
          #  f.write(f_out + "\n")
        
        # after converting CSV to parquet then delete csv files
        prepare_dirs()
  else:
    print("===============================")
    print("The download directory is empty")
    print("===============================")

  # compare files that are already parsed as parquet to those available for download
  _fls_pq # file names of ckw data parsed to parsed to parquet and also pre-processing files
          #   like: 
          #       * "IDS_" files containing unique ids
          #       * "ts_"  files containing time series sets
  fns = set.difference(set(f2parse), set(_fls_pq)) # redundant check 

  print("I will proceed to download a gz file, unzip, parse csv to parquet, and")
  print(" continue file-by-file, deleting redundant files in the process ...")
  print(" a total of {:} files is to be downloaded".format(len(fns)))
  _ = [print(i) for i in fns]

  for fi in fns:
    fi = fi + ".csv.gz" # keep same structre as in file names parsing script
    f = os.path.join(downloads, fi)
    f_csv = os.path.join(storage, fi.replace(".gz", ""))

    # download and unzip
    _curl_output = download(f)
    _gunzip_output = gunzip(f, f_csv)
    
    # parse to csv
    csv2parquet(f_csv)

    # clean up
    if os.path.isfile(f_csv): # remove csv file
      os.remove(f_csv) # TODO: log files that were removed

    if os.path.isfile(f): # remove gz file
      os.remove(f)

    print(" . . . ")